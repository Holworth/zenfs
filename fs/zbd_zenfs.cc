// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <memory>
#include <set>
#include <thread>

#include "fs/io_zenfs.h"
#include "fs/log.h"
#include "fs/metrics.h"
#include "rocksdb/file_system.h"
#include "util/filename.h"
#include "util/string_util.h"
#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <libzbd/zbd.h>
#include <linux/blkzoned.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "fs_zenfs.h"
#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "snapshot.h"
#include "zbd_zenfs.h"

#define KB (1024)
#define MB (1024 * KB)

/* Number of reserved zones for metadata
 * Two non-offline meta zones are needed to be able
 * to roll the metadata log safely. One extra
 * is allocated to cover for one zone going offline.
 */
#define ZENFS_META_ZONES (3)

/* Minimum of number of zones that makes sense */
#define ZENFS_MIN_ZONES (32)

namespace ROCKSDB_NAMESPACE {

Zone::Zone(ZonedBlockDevice *zbd, struct zbd_zone *z)
    : zbd_(zbd),
      busy_(false),
      start_(zbd_zone_start(z)),
      max_capacity_(zbd_zone_capacity(z)),
      wp_(zbd_zone_wp(z)),
      provisioning_zone_(false) {
  lifetime_ = Env::WLTH_NOT_SET;
  used_capacity_ = 0;
  capacity_ = 0;
  if (!(zbd_zone_full(z) || zbd_zone_offline(z) || zbd_zone_rdonly(z)))
    capacity_ = zbd_zone_capacity(z) - (zbd_zone_wp(z) - zbd_zone_start(z));
}

bool Zone::IsUsed() { return (used_capacity_ > 0); }
uint64_t Zone::GetCapacityLeft() { return capacity_; }
bool Zone::IsFull() { return (capacity_ == 0); }
bool Zone::IsEmpty() { return (wp_ == start_); }
uint64_t Zone::GetZoneNr() { return start_ / zbd_->GetZoneSize(); }

void Zone::EncodeJson(std::ostream &json_stream) {
  json_stream << "{";
  json_stream << "\"start\":" << start_ << ",";
  json_stream << "\"capacity\":" << capacity_ << ",";
  json_stream << "\"max_capacity\":" << max_capacity_ << ",";
  json_stream << "\"wp\":" << wp_ << ",";
  json_stream << "\"lifetime\":" << lifetime_ << ",";
  json_stream << "\"used_capacity\":" << used_capacity_;
  json_stream << "}";
}

IOStatus Zone::Reset() {
  ZnsLog(kRed, "Zone%lu is to be reset", ZoneId());
  size_t zone_sz = zbd_->GetZoneSize();
  unsigned int report = 1;
  struct zbd_zone z;
  int ret;

  assert(!IsUsed());
  assert(IsBusy());

  ret = zbd_reset_zones(zbd_->GetWriteFD(), start_, zone_sz);
  if (ret) return IOStatus::IOError("Zone reset failed\n");

  ret = zbd_report_zones(zbd_->GetReadFD(), start_, zone_sz, ZBD_RO_ALL, &z,
                         &report);

  if (ret || (report != 1)) return IOStatus::IOError("Zone report failed\n");

  if (zbd_zone_offline(&z))
    capacity_ = 0;
  else
    max_capacity_ = capacity_ = zbd_zone_capacity(&z);

  wp_ = start_;
  lifetime_ = Env::WLTH_NOT_SET;

  return IOStatus::OK();
}

IOStatus Zone::Finish() {
  size_t zone_sz = zbd_->GetZoneSize();
  int fd = zbd_->GetWriteFD();
  int ret;

  assert(IsBusy());

  ret = zbd_finish_zones(fd, start_, zone_sz);
  if (ret) return IOStatus::IOError("Zone finish failed\n");

  capacity_ = 0;
  wp_ = start_ + zone_sz;

  return IOStatus::OK();
}

IOStatus Zone::Close() {
  size_t zone_sz = zbd_->GetZoneSize();
  int fd = zbd_->GetWriteFD();
  int ret;

  assert(IsBusy());

  if (!(IsEmpty() || IsFull())) {
    ret = zbd_close_zones(fd, start_, zone_sz);
    if (ret) return IOStatus::IOError("Zone close failed\n");
  }

  return IOStatus::OK();
}

IOStatus Zone::Append(char *data, uint32_t size, bool is_gc) {
  ZenFSMetricsLatencyGuard guard(zbd_->GetMetrics(), ZENFS_ZONE_WRITE_LATENCY,
                                 Env::Default());
  // zbd_->GetMetrics()->ReportThroughput(ZENFS_ZONE_WRITE_THROUGHPUT, size);
  // if (is_gc) {
  //   zbd_->GetMetrics()->ReportThroughput(ZENFS_ZONE_GC_WRITE_THROUGHPUT,
  //   size);
  // }

  char *ptr = data;
  uint32_t left = size;
  int fd = zbd_->GetWriteFD();
  int ret;

  if (capacity_ < size) {
    ZnsLog(kRed, "No enought capacity: Left(%llu), Write(%llu)", capacity_,
           size);
    return IOStatus::NoSpace("Not enough capacity for append");
  }

  assert((size % zbd_->GetBlockSize()) == 0);

  // errno = 75, "Value too large for defined data type"
  while (left) {
    ret = pwrite(fd, ptr, left, wp_);
    // printf("[kqh] Zone write %d bytes\n", ret);
    if (ret < 0) {
      printf("[kqh] ZoneAppend Error: %s\n", strerror(errno));
      assert(false);
      return IOStatus::IOError(strerror(errno));
    }

    ptr += ret;
    wp_ += ret;
    capacity_ -= ret;
    left -= ret;

    assert(wp_ <= start_ + max_capacity_);
  }

  zbd_->GetXMetrics()->RecordWriteBytes(size, XZenFSMetricsType::kZNSWrite);

  return IOStatus::OK();
}

IOStatus Zone::CheckRelease() {
  if (!Release()) {
    assert(false);
    return IOStatus::Corruption("Failed to unset busy flag of zone " +
                                std::to_string(GetZoneNr()));
  }

  return IOStatus::OK();
}

zone_id_t Zone::ZoneId() const {
  auto zone_size = zbd_->GetZoneSize();
  if (zone_size == 0) {
    return -1;
  }
  return start_ / zone_size;
}

std::string Zone::ToString() const {
  char buf[256];
  sprintf(buf,
          "ZoneId(%lu) start_=%zu wp=%zu free capacity_=%zu used_=%zu hint=%s "
          "prov=%d",
          ZoneId(), start_, wp_, capacity_, used_capacity_.load(),
          WriteHintToString(this->lifetime_).c_str(), provisioning_zone_);
  return std::string(buf);
}

Zone *ZonedBlockDevice::GetIOZone(uint64_t offset) {
  for (const auto z : io_zones)
    if (z->start_ <= offset && offset < (z->start_ + zone_sz_)) return z;
  return nullptr;
}

ZonedBlockDevice::ZonedBlockDevice(std::string bdevname,
                                   std::shared_ptr<Logger> logger,
                                   std::shared_ptr<ZenFSMetrics> metrics)
    : filename_("/dev/" + bdevname),
      read_f_(-1),
      read_direct_f_(-1),
      write_f_(-1),
      logger_(logger),
      metrics_(metrics) {
  Info(logger_, "New Zoned Block Device: %s", filename_.c_str());
}

std::string ZonedBlockDevice::ErrorToString(int err) {
  char *err_str = strerror(err);
  if (err_str != nullptr) return std::string(err_str);
  return "";
}

IOStatus ZonedBlockDevice::CheckScheduler() {
  std::ostringstream path;
  std::string s = filename_;
  std::fstream f;

  s.erase(0, 5);  // Remove "/dev/" from /dev/nvmeXnY
  path << "/sys/block/" << s << "/queue/scheduler";
  f.open(path.str(), std::fstream::in);
  if (!f.is_open()) {
    return IOStatus::InvalidArgument("Failed to open " + path.str());
  }

  std::string buf;
  getline(f, buf);
  if (buf.find("[mq-deadline]") == std::string::npos) {
    f.close();
    return IOStatus::InvalidArgument(
        "Current ZBD scheduler is not mq-deadline, set it to mq-deadline.");
  }

  f.close();
  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::Open(bool readonly, bool exclusive) {
  struct zbd_zone *zone_rep;
  unsigned int reported_zones;
  uint64_t addr_space_sz;
  zbd_info info;
  Status s;
  uint64_t i = 0;
  uint64_t m = 0;
  // Reserve one zone for metadata and another one for extent migration
  int reserved_zones = 2;
  int ret;

  if (!readonly && !exclusive)
    return IOStatus::InvalidArgument("Write opens must be exclusive");

  /* The non-direct file descriptor acts as an exclusive-use semaphore */
  if (exclusive) {
    read_f_ = zbd_open(filename_.c_str(), O_RDONLY | O_EXCL, &info);
  } else {
    read_f_ = zbd_open(filename_.c_str(), O_RDONLY, &info);
  }

  if (read_f_ < 0) {
    return IOStatus::InvalidArgument(
        "Failed to open zoned block device for read: " + ErrorToString(errno));
  }

  read_direct_f_ = zbd_open(filename_.c_str(), O_RDONLY | O_DIRECT, &info);
  if (read_direct_f_ < 0) {
    return IOStatus::InvalidArgument(
        "Failed to open zoned block device for direct read: " +
        ErrorToString(errno));
  }

  if (readonly) {
    write_f_ = -1;
  } else {
    write_f_ = zbd_open(filename_.c_str(), O_WRONLY | O_DIRECT, &info);
    if (write_f_ < 0) {
      return IOStatus::InvalidArgument(
          "Failed to open zoned block device for write: " +
          ErrorToString(errno));
    }
  }

  if (info.model != ZBD_DM_HOST_MANAGED) {
    return IOStatus::NotSupported("Not a host managed block device");
  }

  if (info.nr_zones < ZENFS_MIN_ZONES) {
    return IOStatus::NotSupported(
        "To few zones on zoned block device (32 required)");
  }

  IOStatus ios = CheckScheduler();
  if (ios != IOStatus::OK()) return ios;

  // xzw: we limit the total zones here to 500
  // info.nr_zones = 500;
  block_sz_ = info.pblock_size;
  zone_sz_ = info.zone_size;
  nr_zones_ = info.nr_zones;
  // nr_provisioning_zones_ = (nr_zones_ - ZENFS_META_ZONES) * 0.07;
  nr_provisioning_zones_ = 1;

  // Set the max open and active zone count to be 14, which is a real
  // hardware parameter. Note that the simulated ZNS SSD does not limit
  // this parameter
  info.max_nr_active_zones = info.max_nr_open_zones = 14;

  if (info.max_nr_active_zones == 0)
    max_nr_active_io_zones_ = info.nr_zones;
  else
    max_nr_active_io_zones_ = info.max_nr_active_zones - reserved_zones;

  if (info.max_nr_open_zones == 0)
    max_nr_open_io_zones_ = info.nr_zones;
  else
    max_nr_open_io_zones_ = info.max_nr_open_zones - reserved_zones;

  Info(logger_, "Zone block device nr zones: %u max active: %u max open: %u \n",
       info.nr_zones, info.max_nr_active_zones, info.max_nr_open_zones);

  addr_space_sz = (uint64_t)nr_zones_ * zone_sz_;

  ret = zbd_list_zones(read_f_, 0, addr_space_sz, ZBD_RO_ALL, &zone_rep,
                       &reported_zones);

  if (ret || reported_zones != nr_zones_) {
    Error(logger_, "Failed to list zones, err: %d", ret);
    return IOStatus::IOError("Failed to list zones");
  }

  while (m < ZENFS_META_ZONES && i < reported_zones) {
    struct zbd_zone *z = &zone_rep[i++];
    /* Only use sequential write required zones */
    if (zbd_zone_type(z) == ZBD_ZONE_TYPE_SWR) {
      if (!zbd_zone_offline(z)) {
        meta_zones.push_back(new Zone(this, z));
      }
      m++;
    }
  }

  active_io_zones_ = 0;
  open_io_zones_ = 0;

  for (; i < reported_zones; i++) {
    struct zbd_zone *z = &zone_rep[i];
    /* Only use sequential write required zones */
    if (zbd_zone_type(z) == ZBD_ZONE_TYPE_SWR) {
      if (!zbd_zone_offline(z)) {
        Zone *newZone = new Zone(this, z);
        if (!newZone->Acquire()) {
          assert(false);
          return IOStatus::Corruption("Failed to set busy flag of zone " +
                                      std::to_string(newZone->GetZoneNr()));
        }
        io_zones.push_back(newZone);
        if (zbd_zone_imp_open(z) || zbd_zone_exp_open(z) ||
            zbd_zone_closed(z)) {
          active_io_zones_++;
          if (zbd_zone_imp_open(z) || zbd_zone_exp_open(z)) {
            if (!readonly) {
              newZone->Close();
            }
          }
        }
        IOStatus status = newZone->CheckRelease();
        if (!status.ok()) return status;
      }
    }
  }

  zone_graphs_.Init(nr_zones_);

  InitWALZones();
  InitKeySSTZones();
  InitEmptyZoneQueue();

  InitializeZoneGCStats();

  InitializePartitions();

  free(zone_rep);
  start_time_ = time(NULL);

  return IOStatus::OK();
}

void ZonedBlockDevice::InitWALZones() {
  // Always use the first two zones for WAL allocation
  wal_zones_.push_back(io_zones[0]);
  wal_zones_.push_back(io_zones[1]);

  wal_zone_active_count_.store(0);
  wal_zone_open_count_.store(0);
}

void ZonedBlockDevice::InitKeySSTZones() {
  // Always use 4 zones preceding wal zones for KeySST allocation
  // The first 3 zones is used to store aggregated level key SSTs.
  //  * One activated aggregated zone and two backup zone preparing for
  //    migration. The reason why we use two backup zones is that the
  //    victim zone may fail to be reset as being written.
  key_sst_zones_.push_back(io_zones[2]);
  key_sst_zones_.push_back(io_zones[3]);
  key_sst_zones_.push_back(io_zones[4]);

  key_sst_zones_.push_back(io_zones[5]);

  // (xzw): should restore the correct active zone
  // (kqh): Continue writing the zone that has been written already
  active_aggr_keysst_zone_ = io_zones[2]->IsEmpty() ? 1 : 0;

  // Pre-allocate one open and active token for key sst zone
  auto token = GetActiveIOZoneTokenIfAvailable();
  assert(token);
  WaitForOpenIOZoneToken(false);

  keysst_aggr_zone_open_count_.store(1);
  keysst_aggr_zone_active_count_.store(1);

  keysst_disaggr_zone_open_count_.store(0);
  keysst_disaggr_zone_active_count_.store(0);

  is_disaggr_zone_written_.store(false);

  ZnsLogKeySSTZoneToken();
}

void ZonedBlockDevice::InitializeZoneGCStats() {
  // For simplicity, the gc stats table is initialized to be a bare state on
  // each Open invoke. This has no relevance to the effect of our GC/placement
  // strategy if we only run it once.
  // However, GC stats table needs to be persisted on each update and are
  // supposed to be intialized from persistent records to achive a full
  // functionality.

  for (int i = 0; i < nr_zones_; ++i) {
    zone_gc_stats_map_.emplace(i, i);
  }
}

void ZonedBlockDevice::InitializePartitions() {
  // TODO: Initialize Hot/Warm partition
  hot_partition_ =
      std::make_shared<HotPartition>(std::unordered_set<zone_id_t>(), this);
  warm_partition_ =
      std::make_shared<HotPartition>(std::unordered_set<zone_id_t>(), this);

  // Initialize each partition assuming the device is in a bare state.
  for (uint32_t i = 0; i < partition_num; ++i) {
    hash_partitions_[i] =
        std::make_shared<HashPartition>(std::unordered_set<zone_id_t>(), this);
    hash_partitions_[i]->SetActivateZone(kInvalidZoneId);
  }
  gc_hist_ = std::make_shared<PartitionGCHist>();
}

std::pair<zone_id_t, double> ZonedBlockDevice::PickZoneWithHighestGarbageRatio(
    const ZonePartition *partition) {
  zone_id_t ret_zone = kInvalidZoneId;
  double max_gr = 0.00;
  for (const auto &zone_id : partition->GetZones()) {
    auto zone = GetZone(zone_id);
    if (!zone->IsFull()) {
      continue;
    }
    double gr = GetZoneGCStatsOf(zone_id)->GarbageRatio();
    if (GetZoneGCStatsOf(zone_id)->GarbageRatio() > max_gr) {
      max_gr = gr;
      ret_zone = zone_id;
    }
  }
  return {ret_zone, max_gr};
}

std::pair<zone_id_t, double> ZonedBlockDevice::PickZoneFromHashPartition(
    uint32_t *partition_id) {
  zone_id_t ret_zone = kInvalidZoneId;

  // First check if there exists some partition that contains an active GC
  // zone. If there is, use this partition preferably to save active token:
  uint32_t partition_with_gc_token = -1;
  for (uint32_t i = 0; i < partition_num; ++i) {
    if (hash_partitions_[i]->HasGCWriteZone()) {
      partition_with_gc_token = i;
    }
  }

  if (partition_with_gc_token != -1) {
    // There is a partition that holds a token for its garbage collection
    // zone. We preferably select this partition for garbage collection
    // due to the considerations of limited number of active zone token.
    //
    // However, a partition might be constantly selected to for garbage
    // collection if it owns a GC write zone from the beginning. This may
    // leave other partitions not touched at all.
    //
    // We check if the partition with active token has been selected for
    // garbage collection consecutively and switch to another partition
    // in a round robin manner
    assert(partition_with_gc_token < partition_num);
    return MaybeSwitchToAnotherPartition(partition_with_gc_token, partition_id);
  } else {
    double max_gr = 0.00;
    for (uint64_t i = 0; i < partition_num; ++i) {
      auto [id, gr] =
          PickZoneWithHighestGarbageRatio(hash_partitions_[i].get());
      if (gr > max_gr) {
        max_gr = gr;
        ret_zone = id;
        *partition_id = i;
      }
    }
    return {ret_zone, max_gr};
  }
}

std::pair<zone_id_t, double> ZonedBlockDevice::MaybeSwitchToAnotherPartition(
    uint32_t partition, uint32_t *choose_partition) {
  assert(partition != -1);
  auto gc_write_zone =
      GetZone(hash_partitions_[partition]->GetCurrGCWriteZone());
  assert(gc_write_zone != nullptr);

  // We can continue choose this partition for garbage collection if it has
  // not been garbage collected for many times. We try picking a zone from
  // this partition for garbage collection if it has such a zone satisfy our
  // gc threshold
  auto gc_count = gc_hist_->LatestGCCount(partition);
  if (gc_count < 5) {
    *choose_partition = partition;
    return PickZoneWithHighestGarbageRatio(hash_partitions_[partition].get());
  }

  // Otherwise we finish the current gc write zone of the previous partition
  // and switch to the next partition
  gc_write_zone->LoopForAcquire();
  auto s = hash_partitions_[partition]->FinishCurrGCWriteZone();
  ZnsLog(kMagenta,
         "MaybeSwitchToAnotherPartition: Finish partition(%lu) gc write zone",
         partition);
  assert(s.ok());
  PutActiveIOZoneToken();
  PutOpenIOZoneToken();

  auto next_partition = (partition + 1) % partition_num;
  *choose_partition = next_partition;
  return PickZoneWithHighestGarbageRatio(
      hash_partitions_[next_partition].get());
}

void ZonedBlockDevice::InitEmptyZoneQueue() {
  for (const auto zone : io_zones) {
    if (zone->IsEmpty() && !IsSpecialZone(zone)) {
      empty_zones_.push_back(zone->ZoneId());
    }
  }
}

// Get one empty zone from empty zone queue. This function must be thread-safe
zone_id_t ZonedBlockDevice::PopEmptyZone() {
  std::lock_guard<std::mutex> lck(empty_zone_mtx_);
  if (empty_zones_.empty()) {
    return kInvalidZoneId;
  }
  auto ret = empty_zones_.front();
  empty_zones_.pop_front();
  return ret;
}

// Push a zone into the empty zone queue. This function must be thread-safe
void ZonedBlockDevice::PushEmptyZone(zone_id_t zone_id) {
  std::lock_guard<std::mutex> lck(empty_zone_mtx_);
  assert(std::find(empty_zones_.begin(), empty_zones_.end(), zone_id) ==
         empty_zones_.end());
  empty_zones_.push_back(zone_id);
}

Zone *ZonedBlockDevice::GetZone(zone_id_t zone_id) {
  // Notice that the first a few zones are used for meta data storage and the
  // zone_id starts from the first zone of the ZonedBlockDevice
  if (zone_id == kInvalidZoneId || zone_id < meta_zones.size()) {
    return nullptr;
  }
  auto id_in_io_zones = zone_id - meta_zones.size();
  auto ret = io_zones[id_in_io_zones];
  assert(ret->ZoneId() == zone_id);
  return ret;
}

uint64_t ZonedBlockDevice::GetFreeSpace() {
  uint64_t free = 0;
  for (const auto z : io_zones) {
    free += z->capacity_;
  }
  return free;
}

uint64_t ZonedBlockDevice::GetUsedSpace() {
  uint64_t used = 0;
  for (const auto z : io_zones) {
    used += z->used_capacity_;
  }
  return used;
}

uint64_t ZonedBlockDevice::GetReclaimableSpace() {
  uint64_t reclaimable = 0;
  for (const auto z : io_zones) {
    if (z->IsFull()) reclaimable += (z->max_capacity_ - z->used_capacity_);
  }
  return reclaimable;
}

uint64_t ZonedBlockDevice::GetOccupySpace() {
  uint64_t occupy = 0;
  for (const auto z : io_zones) {
    occupy += (z->wp_ - z->start_);
  }
  return occupy;
}

double ZonedBlockDevice::GetPartitionGR() {
  uint64_t total_kv = 1, valid_kv = 1;
  for (const auto &zone_id : hash_partitions_[0]->zones) {
    auto gc_stat = GetZoneGCStatsOf(zone_id);
    total_kv += gc_stat->no_kv;
    valid_kv += gc_stat->no_valid_kv;
  }
  return 1 - (double)valid_kv / total_kv;
}

double ZonedBlockDevice::GetPartitionGR(HotnessType type) {
  auto p = GetPartition(type);
  if (p == nullptr) {
    return 0.0;
  }
  uint64_t total_kv = 1, valid_kv = 1;
  for (const auto &zone_id : p->zones) {
    auto gc_stat = GetZoneGCStatsOf(zone_id);
    total_kv += gc_stat->no_kv;
    valid_kv += gc_stat->no_valid_kv;
  }
  return 1 - (double)valid_kv / total_kv;
}

void ZonedBlockDevice::LogZoneStats() {
  uint64_t used_capacity = 0;
  uint64_t reclaimable_capacity = 0;
  uint64_t reclaimables_max_capacity = 0;
  uint64_t active = 0;

  for (const auto z : io_zones) {
    used_capacity += z->used_capacity_;

    if (z->used_capacity_) {
      reclaimable_capacity += z->max_capacity_ - z->used_capacity_;
      reclaimables_max_capacity += z->max_capacity_;
    }

    if (!(z->IsFull() || z->IsEmpty())) active++;
  }

  if (reclaimables_max_capacity == 0) reclaimables_max_capacity = 1;

  Info(logger_,
       "[Zonestats:time(s),used_cap(MB),reclaimable_cap(MB), "
       "avg_reclaimable(%%), active(#), active_zones(#), open_zones(#)] %ld "
       "%lu %lu %lu %lu %ld %ld\n",
       time(NULL) - start_time_, used_capacity / MB, reclaimable_capacity / MB,
       100 * reclaimable_capacity / reclaimables_max_capacity, active,
       active_io_zones_.load(), open_io_zones_.load());
}

void ZonedBlockDevice::LogZoneUsage() {
  for (const auto z : io_zones) {
    int64_t used = z->used_capacity_;

    if (used > 0) {
      Debug(logger_, "Zone 0x%lX used capacity: %ld bytes (%ld MB)\n",
            z->start_, used, used / MB);
    }
  }
}

void ZonedBlockDevice::LogGarbageInfo() {
  // Log zone garbage stats vector.
  //
  // The values in the vector represents how many zones with target garbage
  // percent. Garbage percent of each index: [0%, <10%, < 20%, ... <100%, 100%]
  // For example `[100, 1, 2, 3....]` means 100 zones are empty, 1 zone has less
  // than 10% garbage, 2 zones have  10% ~ 20% garbage ect.
  //
  // We don't need to lock io_zones since we only read data and we don't need
  // the result to be precise.
  int zone_gc_stat[25] = {0};
  for (auto z : io_zones) {
    if (z->IsEmpty()) {
      zone_gc_stat[0]++;
      continue;
    }

    if (!z->Acquire()) {
      continue;
    }

    double garbage_rate =
        double(z->wp_ - z->start_ - z->used_capacity_) / z->max_capacity_;
    assert(garbage_rate >= 0);
    // assert(garbage_rate <= 2);
    if (garbage_rate >= 2) {
      continue;
    }
    int idx = int((garbage_rate + 0.1) * 10);
    zone_gc_stat[idx]++;

    z->Release();
  }

  std::stringstream ss;
  ss << "Zone Garbage Stats: [";
  for (int i = 0; i < 12; i++) {
    ss << zone_gc_stat[i] << " ";
  }
  ss << "]";
  Info(logger_, "%s", ss.str().data());
}

ZonedBlockDevice::~ZonedBlockDevice() {
  for (const auto z : meta_zones) {
    delete z;
  }

  for (const auto z : io_zones) {
    delete z;
  }

  zbd_close(read_f_);
  zbd_close(read_direct_f_);
  zbd_close(write_f_);
}

#define LIFETIME_DIFF_NOT_GOOD (100)
#define LIFETIME_DIFF_COULD_BE_WORSE (50)

unsigned int GetLifeTimeDiff(Env::WriteLifeTimeHint zone_lifetime,
                             Env::WriteLifeTimeHint file_lifetime) {
  assert(file_lifetime <= Env::WLTH_EXTREME);

  if ((file_lifetime == Env::WLTH_NOT_SET) ||
      (file_lifetime == Env::WLTH_NONE)) {
    if (file_lifetime == zone_lifetime) {
      return 0;
    } else {
      return LIFETIME_DIFF_NOT_GOOD;
    }
  }

  // (kqh): For a zone with lifetime not set, you can fill in any possible
  // files, thus the diff is 0
  if (zone_lifetime == Env::WLTH_NOT_SET) return 0;
  if (zone_lifetime > file_lifetime) return zone_lifetime - file_lifetime;
  // if (zone_lifetime == file_lifetime) return LIFETIME_DIFF_COULD_BE_WORSE;
  if (zone_lifetime == file_lifetime) return 0;

  return LIFETIME_DIFF_NOT_GOOD;
}

IOStatus ZonedBlockDevice::AllocateMetaZone(Zone **out_meta_zone) {
  assert(out_meta_zone);
  *out_meta_zone = nullptr;
  ZenFSMetricsLatencyGuard guard(metrics_, ZENFS_META_ALLOC_LATENCY,
                                 Env::Default());
  metrics_->ReportQPS(ZENFS_META_ALLOC_QPS, 1);

  for (const auto z : meta_zones) {
    /* If the zone is not used, reset and use it */
    if (z->Acquire()) {
      if (!z->IsUsed()) {
        if (!z->IsEmpty() && !z->Reset().ok()) {
          Warn(logger_, "Failed resetting zone!");
          IOStatus status = z->CheckRelease();
          if (!status.ok()) return status;
          continue;
        }
        *out_meta_zone = z;
        return IOStatus::OK();
      }
    }
  }
  assert(true);
  Error(logger_, "Out of metadata zones, we should go to read only now.");
  return IOStatus::NoSpace("Out of metadata zones");
}

IOStatus ZonedBlockDevice::ResetUnusedIOZones() {
  // TODO: Some zones may need to used for provisioning
  for (const auto z : io_zones) {
    if (IsSpecialZone(z) || IsValueSSTZone(z)) {
      continue;
    }

    if (z->Acquire()) {
      // IsEmpty() means this zone has not been written yet
      // IsUsed() means there is valid data in this zone
      if (!z->IsEmpty() && !z->IsUsed()) {
        printf("[kqh] Reset zone: %s\n", z->ToString().c_str());
        bool full = z->IsFull();
        IOStatus reset_status = z->Reset();
        IOStatus release_status = z->CheckRelease();
        if (!release_status.ok()) return release_status;
        if (!full) PutActiveIOZoneToken();
      } else {
        IOStatus release_status = z->CheckRelease();
        if (!release_status.ok()) return release_status;
      }
    }
  }
  return IOStatus::OK();
}

void ZonedBlockDevice::WaitForOpenIOZoneToken(bool prioritized) {
  /*
  printf("[deadlock] WaitForOpenIOZoneToken prioritized=%d tid=%d\n",
         prioritized, std::this_thread::get_id()); */
  long allocator_open_limit;

  /* Avoid non-priortized allocators from starving prioritized ones */
  if (prioritized) {
    allocator_open_limit = max_nr_open_io_zones_;
  } else {
    allocator_open_limit = max_nr_open_io_zones_ - 1;
  }

  ZnsLog(kMagenta, "WaitForOpenIOZoneToken: %ld, Limit: %ld",
         open_io_zones_.load(), allocator_open_limit);

  /* Wait for an open IO Zone token - after this function returns
   * the caller is allowed to write to a closed zone. The callee
   * is responsible for calling a PutOpenIOZoneToken to return the resource
   */
  std::unique_lock<std::mutex> lk(zone_resources_mtx_);
  zone_resources_.wait(lk, [this, allocator_open_limit] {
    auto open_io_zones = open_io_zones_.load();
    /*
    printf("[deadlock] current open_io_zones:%ld need:%ld tid=%d\n",
           open_io_zones, allocator_open_limit, std::this_thread::get_id());
           */
    if (open_io_zones_.load() < allocator_open_limit) {
      open_io_zones_++;
      return true;
    } else {
      return false;
    }
  });
  /*
  printf("[deadlock] WaitForOpenIOZoneToken prioritized=%d Done tid=%d\n",
         prioritized, std::this_thread::get_id());
  */
}

bool ZonedBlockDevice::GetActiveIOZoneTokenIfAvailable() {
  /* Grap an active IO Zone token if available - after this function returns
   * the caller is allowed to write to a closed zone. The callee
   * is responsible for calling a PutActiveIOZoneToken to return the resource
   */
  ZnsLog(kMagenta, ">>>> GetActiveIOZoneTokenIfAvailable: %ld, limit=%ld",
         active_io_zones_.load(), max_nr_active_io_zones_);

  if (active_io_zones_.load() == max_nr_active_io_zones_) {
    assert(false);
  }
  std::unique_lock<std::mutex> lk(zone_resources_mtx_);
  if (active_io_zones_.load() < max_nr_active_io_zones_) {
    active_io_zones_++;
    assert(active_io_zones_ >= 0 &&
           active_io_zones_ <= max_nr_active_io_zones_);
    return true;
  }
  return false;
}

void ZonedBlockDevice::PutOpenIOZoneToken() {
  ZnsLog(kMagenta, "PutOpenIOZoneToken: %ld", open_io_zones_.load());
  {
    std::unique_lock<std::mutex> lk(zone_resources_mtx_);
    open_io_zones_--;
  }
  // (kqh): Replace notify_one() with notify_all()
  zone_resources_.notify_one();
}

void ZonedBlockDevice::PutActiveIOZoneToken() {
  ZnsLog(kMagenta, "<<<< PutActiveIOZoneToken: %ld", active_io_zones_.load());
  {
    std::unique_lock<std::mutex> lk(zone_resources_mtx_);
    active_io_zones_--;
    assert(active_io_zones_ >= 0 &&
           active_io_zones_ <= max_nr_active_io_zones_);
  }
  // (kqh): Replace notify_one() with notify_all()
  zone_resources_.notify_one();
}

IOStatus ZonedBlockDevice::ApplyFinishThreshold() {
  IOStatus s;

  if (finish_threshold_ == 0) return IOStatus::OK();

  for (const auto z : io_zones) {
    if (IsSpecialZone(z) || IsValueSSTZone(z)) {
      continue;
    }

    if (z->Acquire()) {
      bool within_finish_threshold =
          z->capacity_ < (z->max_capacity_ * finish_threshold_ / 100);
      if (!(z->IsEmpty() || z->IsFull()) && within_finish_threshold) {
        /* If there is less than finish_threshold_% remaining capacity in a
         * non-open-zone, finish the zone */
        s = z->Finish();
        if (!s.ok()) {
          z->Release();
          Debug(logger_, "Failed finishing zone");
          return s;
        }
        s = z->CheckRelease();
        if (!s.ok()) return s;
        PutActiveIOZoneToken();
      } else {
        s = z->CheckRelease();
        if (!s.ok()) return s;
      }
    }
  }

  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::FinishCheapestIOZone() {
  printf("[kqh] FinishCheapestIOZone Start\n");
  IOStatus s;
  Zone *finish_victim = nullptr;

  for (const auto z : io_zones) {
    if (IsSpecialZone(z) || IsValueSSTZone(z)) {
      continue;
    }

    if (z->Acquire()) {
      // Empty and Full Zone does not occupy Active Zone Token
      if (z->IsEmpty() || z->IsFull()) {
        s = z->CheckRelease();
        if (!s.ok()) return s;
        continue;
      }
      if (finish_victim == nullptr) {
        finish_victim = z;
        continue;
      }
      if (finish_victim->capacity_ > z->capacity_) {
        s = finish_victim->CheckRelease();
        if (!s.ok()) return s;
        finish_victim = z;
      } else {
        s = z->CheckRelease();
        if (!s.ok()) return s;
      }
    }
  }

  // If all non-busy zones are empty or full, we should return success.
  if (finish_victim == nullptr) {
    Info(logger_, "All non-busy zones are empty or full, skip.");
    return IOStatus::OK();
  }

  printf("[kqh] Finish Zone: %s\n", finish_victim->ToString().c_str());
  s = finish_victim->Finish();
  IOStatus release_status = finish_victim->CheckRelease();

  // (kqh) Why do not return the open zone token of this zone?
  if (s.ok()) {
    PutActiveIOZoneToken();
  }

  if (!release_status.ok()) {
    return release_status;
  }

  return s;
}

IOStatus ZonedBlockDevice::GetBestOpenZoneMatch(
    Env::WriteLifeTimeHint file_lifetime, unsigned int *best_diff_out,
    Zone **zone_out, uint32_t min_capacity) {
  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  Zone *allocated_zone = nullptr;
  IOStatus s;

  ZnsLog(kGreen, "[kqh]: Allocate Zone for BestMatch, hint: %s",
         WriteHintToString(file_lifetime).c_str());

  for (const auto z : io_zones) {
    if (IsSpecialZone(z) || IsValueSSTZone(z)) {
      continue;
    }

    if (z->Acquire()) {
      // printf("[kqh] Zone: %s\n", z->ToString().c_str());
      // (kqh): Skip this zone if it is marked as provisioning zone
      if (z->IsProvisioningZone()) {
        s = z->CheckRelease();
        if (!s.ok()) return s;
        continue;
      }
      if ((z->used_capacity_ > 0) && !z->IsFull() &&
          z->capacity_ >= min_capacity) {
        unsigned int diff = GetLifeTimeDiff(z->lifetime_, file_lifetime);
        // printf("[kqh] Zone(%d) Diff = %d BestDiff = %d Zone(%s)\n",
        // z->ZoneId(), diff, best_diff, z->ToString().c_str());
        if (diff <= best_diff) {
          if (allocated_zone != nullptr) {
            s = allocated_zone->CheckRelease();
            if (!s.ok()) {
              IOStatus s_ = z->CheckRelease();
              if (!s_.ok()) return s_;
              return s;
            }
          }
          allocated_zone = z;
          best_diff = diff;
        } else {
          s = z->CheckRelease();
          if (!s.ok()) return s;
        }
      } else {
        s = z->CheckRelease();
        if (!s.ok()) return s;
      }
    }
  }

  *best_diff_out = best_diff;
  *zone_out = allocated_zone;
  ZnsLog(kGreen, "[kqh] Returned Allocated Zone: %d best diff=%d (%s)",
         allocated_zone ? allocated_zone->ZoneId() : -1, best_diff,
         allocated_zone ? allocated_zone->ToString().c_str() : "");

  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::AllocateEmptyZone(Zone **zone_out) {
  IOStatus s;
  Zone *allocated_zone = nullptr;
  for (const auto z : io_zones) {
    if (IsSpecialZone(z) || IsValueSSTZone(z)) {
      continue;
    }

    if (z->Acquire()) {
      if (z->IsProvisioningZone()) {
        s = z->CheckRelease();
        if (!s.ok()) return s;
        continue;
      }
      if (z->IsEmpty()) {
        allocated_zone = z;
        break;
      } else {
        s = z->CheckRelease();
        if (!s.ok()) return s;
      }
    }
  }
  *zone_out = allocated_zone;
  return IOStatus::OK();
}

int ZonedBlockDevice::DirectRead(char *buf, uint64_t offset, int n) {
  int ret = 0;
  int left = n;
  int r = -1;
  int f = GetReadDirectFD();

  GetMetrics()->ReportThroughput(ZENFS_ZONE_READ_THROUGHPUT, n);

  while (left) {
    r = pread(f, buf, left, offset);
    if (r <= 0) {
      if (r == -1 && errno == EINTR) {
        continue;
      }
      break;
    }
    ret += r;
    buf += r;
    left -= r;
    offset += r;
  }

  if (r < 0) return r;
  return ret;
}

IOStatus ZonedBlockDevice::ReleaseMigrateZone(Zone *zone) {
  IOStatus s = IOStatus::OK();
  {
    std::unique_lock<std::mutex> lock(migrate_zone_mtx_);
    migrating_ = false;
    if (zone != nullptr) {
      s = zone->CheckRelease();
      Info(logger_, "ReleaseMigrateZone: %lu", zone->start_);
    }
  }
  migrate_resource_.notify_one();
  return s;
}

IOStatus ZonedBlockDevice::TakeMigrateZone(Zone **out_zone,
                                           Env::WriteLifeTimeHint file_lifetime,
                                           uint32_t min_capacity) {
  printf("[kqh] TakeMigrateZone Start");
  std::unique_lock<std::mutex> lock(migrate_zone_mtx_);
  migrate_resource_.wait(lock, [this] { return !migrating_; });

  migrating_ = true;

  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  auto s =
      GetBestOpenZoneMatch(file_lifetime, &best_diff, out_zone, min_capacity);
  if (s.ok() && (*out_zone) != nullptr) {
    Info(logger_, "TakeMigrateZone: %lu", (*out_zone)->start_);
  } else {
    migrating_ = false;
  }
  return s;
}

IOStatus ZonedBlockDevice::AllocateWALZone(Zone **out_zone,
                                           WALZoneAllocationHint *hint) {
  ZnsLog(kRed, "[kqh] AllocateWALZone(active=%d): zone[0]=%s\nzone[1]=%s",
         active_wal_zone_, wal_zones_[0]->ToString().c_str(),
         wal_zones_[1]->ToString().c_str());

  ResetUnusedWALZones();
  if (active_wal_zone_ == kNoActiveWALZone) {
    assert(wal_zones_[0]->IsEmpty() || wal_zones_[1]->IsEmpty());
    if (wal_zones_[0]->IsEmpty() && wal_zones_[1]->IsEmpty()) {
      active_wal_zone_ = 0;
    } else if (!wal_zones_[0]->IsEmpty()) {
      active_wal_zone_ = 0;
    } else if (!wal_zones_[1]->IsEmpty()) {
      active_wal_zone_ = 1;
    }
    if (active_wal_zone_ == kNoActiveWALZone) {
      return IOStatus::NoSpace("No Space for WAL write");
    }
  }

  // (xzw): An ActiveToken should be taken at the first allocation
  WaitForOpenIOZoneToken(true);
  wal_zone_open_count_++;
  ZnsLog(kMagenta, "WALOpenZoneCount(%d) WALActiveZoneCount(%d)",
         wal_zone_open_count_.load(), wal_zone_active_count_.load());

  // Directly return if the active zone has enough space
  auto ret = wal_zones_[active_wal_zone_];
  if (ret->GetCapacityLeft() > GetBlockSize()) {
    *out_zone = ret;
    (*out_zone)->LoopForAcquire();
    // (xzw): ensure an active zone is held
    if ((*out_zone)->IsEmpty()) {
      auto get_token = GetActiveIOZoneTokenIfAvailable();
      assert(get_token);
      wal_zone_active_count_++;
      ZnsLog(kMagenta, "WALOpenZoneCount(%d) WALActiveZoneCount(%d)",
             wal_zone_open_count_.load(), wal_zone_active_count_.load());
    }
    return IOStatus::OK();
  }

  // Otherwise open a new WAL Zone
  auto s = SwitchWALZone();
  if (!s.ok()) {
    PutOpenIOZoneToken();
    wal_zone_open_count_--;
    ZnsLog(kMagenta, "WALOpenZoneCount(%d) WALActiveZoneCount(%d)",
           wal_zone_open_count_.load(), wal_zone_active_count_.load());
    return s;
  }
  *out_zone = wal_zones_[active_wal_zone_];
  // (kqh) Loop until get the write ownership of this zone
  (*out_zone)->LoopForAcquire();
  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::SwitchWALZone() {
  ZnsLog(kMagenta, "SwitchWALZone");
  auto next_wal_zone = wal_zones_[GetAdvancedActiveWALZoneIndex()];
  assert(next_wal_zone->IsEmpty() && !next_wal_zone->IsBusy());

  auto get_token = GetActiveIOZoneTokenIfAvailable();
  if (!get_token) {
    return IOStatus::NoSpace("WALAllocation Get Active Zone Token failed");
  }
  wal_zone_active_count_++;
  ZnsLog(kMagenta, "WALOpenZoneCount(%d) WALActiveZoneCount(%d)",
         wal_zone_open_count_.load(), wal_zone_active_count_.load());
  // (kqh): May need to know all valid file extents in this zone
  active_wal_zone_ = GetAdvancedActiveWALZoneIndex();
  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::ResetUnusedWALZones() {
  decltype(wal_zones_.size()) i = 0;
  for (i = 0; i < wal_zones_.size(); ++i) {
    if (i == active_wal_zone_) {
      continue;
    }
    auto z = wal_zones_[i];
    if (wal_zones_[i]->Acquire()) {
      if (!z->IsEmpty() && !z->IsUsed()) {
        ZnsLog(kBlue, "[kqh] ResetUnusedWALZones: reset zone(%s)",
               z->ToString().c_str());
        bool full = z->IsFull();
        IOStatus reset_status = z->Reset();
        IOStatus release_status = z->CheckRelease();
        if (!release_status.ok()) return release_status;
        if (!full) {
          // Why put active IO Token only when not full?
          PutActiveIOZoneToken();
          wal_zone_active_count_--;
          ZnsLog(kMagenta, "WALOpenZoneCount(%d) WALActiveZoneCount(%d)",
                 wal_zone_open_count_.load(), wal_zone_active_count_.load());
        }
      } else {
        IOStatus release_status = z->CheckRelease();
        if (!release_status.ok()) return release_status;
      }
    }
  }
  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::ResetUnusedKeySSTZones() {
  decltype(key_sst_zones_.size()) i = 0;
  for (i = 0; i < key_sst_zones_.size(); ++i) {
    if (i == active_aggr_keysst_zone_) {
      continue;
    }
    auto z = key_sst_zones_[i];
    if (z->Acquire()) {
      if (!z->IsEmpty() && !z->IsUsed()) {
        ZnsLog(kBlue, "[kqh] ResetUnusedKeySSTZones: reset zone(%s)\n",
               z->ToString().c_str());
        bool full = z->IsFull();
        IOStatus reset_status = z->Reset();
        IOStatus release_status = z->CheckRelease();
        if (!release_status.ok()) return release_status;
        if (!full) {
          PutActiveIOZoneToken();
          keysst_aggr_zone_active_count_--;
        }
        PutOpenIOZoneToken();
        keysst_aggr_zone_open_count_--;

        ZnsLogKeySSTZoneToken();
      } else {
        IOStatus release_status = z->CheckRelease();
        if (!release_status.ok()) return release_status;
      }
    }
  }
  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::AllocateIOZone(Env::WriteLifeTimeHint file_lifetime,
                                          IOType io_type, Zone **out_zone) {
  Zone *allocated_zone = nullptr;
  Zone *empty_zone = nullptr;
  unsigned int best_diff = LIFETIME_DIFF_NOT_GOOD;
  int new_zone = 0;
  IOStatus s;

  auto tag = ZENFS_WAL_IO_ALLOC_LATENCY;
  if (io_type != IOType::kWAL) {
    // L0 flushes have lifetime MEDIUM
    if (file_lifetime == Env::WLTH_MEDIUM) {
      tag = ZENFS_L0_IO_ALLOC_LATENCY;
    } else {
      tag = ZENFS_NON_WAL_IO_ALLOC_LATENCY;
    }
  }

  ZenFSMetricsLatencyGuard guard(metrics_, tag, Env::Default());
  metrics_->ReportQPS(ZENFS_IO_ALLOC_QPS, 1);

  // Check if a deferred IO error was set
  s = GetZoneDeferredStatus();
  if (!s.ok()) {
    return s;
  }

  if (io_type != IOType::kWAL) {
    s = ApplyFinishThreshold();
    if (!s.ok()) {
      return s;
    }
  }

  ZnsLog(Color::kBlue, "[xzw] Start Allocate IO Zone, tid=%d",
         (std::this_thread::get_id()));

  WaitForOpenIOZoneToken(io_type == IOType::kWAL);

  /* Try to fill an already open zone(with the best life time diff) */
  s = GetBestOpenZoneMatch(file_lifetime, &best_diff, &allocated_zone);
  if (!s.ok()) {
    PutOpenIOZoneToken();
    return s;
  }

  // Holding allocated_zone if != nullptr

  if (best_diff >= LIFETIME_DIFF_COULD_BE_WORSE) {
    bool got_token = GetActiveIOZoneTokenIfAvailable();

    /* If we did not get a token, try to use the best match, even if the life
     * time diff not good but a better choice than to finish an existing zone
     * and open a new one
     */
    if (allocated_zone != nullptr) {
      if (!got_token && best_diff == LIFETIME_DIFF_COULD_BE_WORSE) {
        Debug(logger_,
              "Allocator: avoided a finish by relaxing lifetime diff "
              "requirement\n");
      } else {
        s = allocated_zone->CheckRelease();
        if (!s.ok()) {
          PutOpenIOZoneToken();
          if (got_token) PutActiveIOZoneToken();
          return s;
        }
        allocated_zone = nullptr;
      }
    }

    /* If we haven't found an open zone to fill, open a new zone */
    if (allocated_zone == nullptr) {
      ZnsLog(kGreen, "[kqh] Try Allocating from an Empty Zone");
      /* We have to make sure we can open an empty zone */
      while (!got_token && !GetActiveIOZoneTokenIfAvailable()) {
        s = FinishCheapestIOZone();
        if (!s.ok()) {
          PutOpenIOZoneToken();
          return s;
        }
      }

      s = AllocateEmptyZone(&allocated_zone);
      if (!s.ok()) {
        PutActiveIOZoneToken();
        PutOpenIOZoneToken();
        return s;
      }

      if (allocated_zone != nullptr) {
        assert(allocated_zone->IsBusy());
        allocated_zone->lifetime_ = file_lifetime;
        new_zone = true;
      } else {
        PutActiveIOZoneToken();
      }
    }
  }

  // xzw: use the unmatched zone if empty zone allocation fails
  if (allocated_zone == nullptr) {
    /* Try to fill an already open zone(with the best life time diff) */
    printf("[xzw]: Entered unmatch zone branch\n");
    s = GetBestOpenZoneMatch(file_lifetime, &best_diff, &allocated_zone);
    if (!s.ok()) {
      PutOpenIOZoneToken();
      return s;
    }
    printf("[xzw]: Unmatched zone allocated Done.\n");
  }

  if (allocated_zone) {
    assert(allocated_zone->IsBusy());
    Debug(logger_,
          "Allocating zone(new=%d) start: 0x%lx wp: 0x%lx lt: %d file lt: %d\n",
          new_zone, allocated_zone->start_, allocated_zone->wp_,
          allocated_zone->lifetime_, file_lifetime);
  } else {
    PutOpenIOZoneToken();
  }

  if (io_type != IOType::kWAL) {
    LogZoneStats();
  }

  *out_zone = allocated_zone;
  if (allocated_zone && allocated_zone->lifetime_ == Env::WLTH_NOT_SET) {
    allocated_zone->lifetime_ = file_lifetime;
  }

  metrics_->ReportGeneral(ZENFS_OPEN_ZONES_COUNT, open_io_zones_);
  metrics_->ReportGeneral(ZENFS_ACTIVE_ZONES_COUNT, active_io_zones_);

  /*
  if (io_type == IOType::kWAL) {
    Info(logger_, "Allocate zone for WAL: ZoneID()=%d\n",
         allocated_zone->ZoneId());
  }
  */

  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::AllocateKeySSTZone(Zone **out_zone,
                                              KeySSTZoneAllocationHint *hint) {
  assert(hint->level_ >= 0);
  ZnsLog(kGreen, "[kqh] AllocateKeySSTZone (file=%s size=%u level=%d)",
         hint->filename_.c_str(), hint->size_, hint->level_);

  // Not aggregated levels
  if (!IsAggregatedLevel(hint->level_)) {
    // For disaggregated levels, acquire the open token and active token when
    // the first write happens.
    if (!is_disaggr_zone_written_) {
      WaitForOpenIOZoneToken(false);
      while (!GetActiveIOZoneTokenIfAvailable())
        ;
      is_disaggr_zone_written_ = true;
    }

    ZnsLogKeySSTZoneToken();

    *out_zone = key_sst_zones_.back();
    (*out_zone)->LoopForAcquire();
    ZnsLog(kGreen, "AllocateKeySSTZone (file=%s): return Zone%d",
           hint->filename_.c_str(), (*out_zone)->ZoneId());
    return IOStatus::OK();
  }

  // Aggregated Level Zone allocation
  Zone *ret_zone = nullptr;
  while (true) {
    auto old_active_aggr_keysst_zone = active_aggr_keysst_zone_;
    ret_zone = key_sst_zones_[old_active_aggr_keysst_zone];
    ret_zone->LoopForAcquire();

    ZnsLog(kGreen,
           "AllocateKeySSTZone (file=%s) Zone %d LoopForAcquire Finished",
           hint->filename_.c_str(), ret_zone->ZoneId());

    // After acquiring the ownership of this zone, another might have
    // already changed the active key sst zone, thus a double check is
    // needed to detect such a case
    if (old_active_aggr_keysst_zone != active_aggr_keysst_zone_) {
      ret_zone->CheckRelease();
      continue;
    }

    if (ret_zone->GetCapacityLeft() >= hint->size_) {
      ZnsLog(kGreen,
             "AllocateKeySSTZone (file=%s) Zone %d: capacity: %lu, allocation "
             "size: %lu",
             hint->filename_.c_str(), ret_zone->ZoneId(),
             ret_zone->GetCapacityLeft(), hint->size_);
      break;
    }

    // It is impossible that two threads do the same migration task here
    auto s = MigrateAggregatedLevelZone();
    if (!s.ok()) {
      return s;
    }
  }
  *out_zone = ret_zone;
  ZnsLog(kGreen, "AllocateKeySSTZone (file=%s): Return Zone%d",
         hint->filename_.c_str(), (*out_zone)->ZoneId());
  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::MigrateAggregatedLevelZone() {
  assert(zenfs_ != nullptr);

  auto src_zone = key_sst_zones_[active_aggr_keysst_zone_];

  if (src_zone->IsEmpty()) {
    ZnsLog(Color::kDefault, "[xzw] Migration skipped zone %d",
           src_zone->ZoneId());
    return IOStatus::OK();
  }

  // Reset any possible unused zone before migration
  ResetUnusedKeySSTZones();

  assert(src_zone->IsBusy());

  // Find an empty zone that can accommadate migrated extents
  // auto next_zone = (active_aggr_keysst_zone_ + 1) % 2;
  auto next_zone = active_aggr_keysst_zone_;
  for (int i = 1; i < 3; ++i) {
    auto idx = (next_zone + i) % 3;
    if (key_sst_zones_[idx]->IsEmpty()) {
      next_zone = (next_zone + i) % 3;
      break;
    }
  }

  // Migration fails: no empty zone can be used as migration destination
  if (next_zone == active_aggr_keysst_zone_) {
    assert(false);
    abort();
  }

  auto dst_zone = key_sst_zones_[next_zone];

  ZnsLog(Color::kBlue, "[xzw] Migrating from zone %d to zone %d",
         src_zone->ZoneId(), dst_zone->ZoneId());

  assert(dst_zone->IsEmpty());

  //
  // Acquire active and open token for the destination zone of this migration
  // task. Once the migration is done, put the active and open token of src
  // zone back. The destination zone takes the active/open token and will
  // absort incoming key sst writes
  //

  ZnsLog(kYellow, "Open zones %lu and active zones %lu in %s",
         open_io_zones_.load(), active_io_zones_.load(), __FUNCTION__);
  WaitForOpenIOZoneToken(false);
  while (!GetActiveIOZoneTokenIfAvailable())
    ;
  keysst_aggr_zone_open_count_ += 1;
  keysst_aggr_zone_active_count_ += 1;

  ZnsLogKeySSTZoneToken();

  auto s = zenfs_->MigrateAggregatedLevelZone(src_zone, dst_zone);
  if (!s.ok()) {
    PutOpenIOZoneToken();
    keysst_aggr_zone_open_count_--;

    PutActiveIOZoneToken();
    keysst_aggr_zone_active_count_--;
    src_zone->CheckRelease();
    ZnsLog(kRed, "MigrateAggregatedLevelZone Fails. [Error: %s]",
           s.ToString().c_str());

    ZnsLogKeySSTZoneToken();
    return s;
  }

  // Change current active zone must be done when src_zone is locked.

  active_aggr_keysst_zone_ = next_zone;
  ZnsLog(Color::kRed, "Updating active_aggr_keysst_zone_ to %d in superblock",
         active_aggr_keysst_zone_);

  //
  // Reset the source zone immediately (if possible) if migration is finished.
  // The reset operation, migration and change of activated zone pointer should
  // be combined as an atomic operation so that the concurrent writer should
  // detect the activated zone has been changed.
  //
  if (!src_zone->IsEmpty() && !src_zone->IsUsed()) {
    ZnsLog(kBlue, "[kqh] ResetUnusedKeySSTZones: reset zone(%s)\n",
           src_zone->ToString().c_str());
    bool full = src_zone->IsFull();
    IOStatus reset_status = src_zone->Reset();
    if (!reset_status.ok()) {
      src_zone->CheckRelease();
      return reset_status;
    }
    PutOpenIOZoneToken();
    keysst_aggr_zone_open_count_--;
    if (!full) {
      PutActiveIOZoneToken();
      keysst_aggr_zone_active_count_--;
    }

    ZnsLogKeySSTZoneToken();
  }

  src_zone->CheckRelease();
  ZnsLog(Color::kBlue, "[xzw] Migration finished", src_zone->ZoneId(),
         dst_zone->ZoneId());

  // s = ResetUnusedKeySSTZones();
  // if (!s.ok()) {
  //   return s;
  // }
  return IOStatus::OK();
}

IOStatus ZonedBlockDevice::AllocateValueSSTZone(
    Zone **out_zone, ValueSSTZoneAllocationHint *hint) {
  ZnsLog(kGreen, "[kqh] Allocate Value SST");
  auto s = AllocateIOZone(Env::WLTH_EXTREME, IOType::kValueSST, out_zone);
  return IOStatus::OK();
}

std::string ZonedBlockDevice::GetFilename() { return filename_; }

uint32_t ZonedBlockDevice::GetBlockSize() { return block_sz_; }

void ZonedBlockDevice::EncodeJsonZone(std::ostream &json_stream,
                                      const std::vector<Zone *> zones) {
  bool first_element = true;
  json_stream << "[";
  for (Zone *zone : zones) {
    if (first_element) {
      first_element = false;
    } else {
      json_stream << ",";
    }
    zone->EncodeJson(json_stream);
  }

  json_stream << "]";
}

void ZonedBlockDevice::EncodeJson(std::ostream &json_stream) {
  json_stream << "{";
  json_stream << "\"meta\":";
  EncodeJsonZone(json_stream, meta_zones);
  json_stream << ",\"io\":";
  EncodeJsonZone(json_stream, io_zones);
  json_stream << "}";
}

IOStatus ZonedBlockDevice::GetZoneDeferredStatus() {
  std::lock_guard<std::mutex> lock(zone_deferred_status_mutex_);
  return zone_deferred_status_;
}

void ZonedBlockDevice::SetZoneDeferredStatus(IOStatus status) {
  std::lock_guard<std::mutex> lk(zone_deferred_status_mutex_);
  if (!zone_deferred_status_.ok()) {
    zone_deferred_status_ = status;
  }
}

void ZonedBlockDevice::GetZoneSnapshot(std::vector<ZoneSnapshot> &snapshot) {
  for (auto *zone : io_zones) {
    // (XZW): Ignore special zone since a GC job might operate on zones
    // reported from this function. However, the reclaimation of WALZone and
    // KeySSTZone are handled internally by ZenFS instead of DB. We ignore
    // these special zones to avoid DB reclaimed them.
    if (IsSpecialZone(zone)) {
      continue;
    }
    snapshot.emplace_back(*zone);
  }
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)
