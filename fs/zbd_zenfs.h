// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#include <memory>
#include <unordered_map>
#if !defined(ROCKSDB_LITE) && defined(OS_LINUX)

#include <errno.h>
#include <libzbd/zbd.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "log.h"
#include "metrics.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"

namespace ROCKSDB_NAMESPACE {

namespace config {
  const static int kAggrLevelThreshold = 3;
};

inline std::string IOTypeToString(IOType type) {
  switch (type) {
    case IOType::kFlushFile:
      return "FlushFile";
    case IOType::kCompactionOutputFile:
      return "kCompactionOutputFile";
    case IOType::kWAL:
      return "kWAL";
    default:
      return "Unknown";
  }
}

inline std::string WriteHintToString(Env::WriteLifeTimeHint hint) {
  switch (hint) {
    case Env::WLTH_NOT_SET:
      return "NOT_SET";
    case Env::WLTH_NONE:
      return "NONE";
    case Env::WLTH_SHORT:
      return "SHORT";
    case Env::WLTH_MEDIUM:
      return "MEDIUM";
    case Env::WLTH_LONG:
      return "LONG";
    case Env::WLTH_EXTREME:
      return "EXTREME";
    default:
      return "ERROR_HINT";
  }
}

class ZonedBlockDevice;
class ZoneSnapshot;
class ZenFSSnapshotOptions;
class ZenFS;

class ZoneFile;

// Hint for Zone Allocation
struct ZoneAllocationHint {};
struct WALZoneAllocationHint : public ZoneAllocationHint {
 public:
  WALZoneAllocationHint() = default;
  WALZoneAllocationHint(size_t size, ZoneFile *file)
      : len(size), zone_file(file) {}
  size_t len;
  ZoneFile *zone_file;
};

struct ValueSSTZoneAllocationHint : public ZoneAllocationHint {};

struct KeySSTZoneAllocationHint : public ZoneAllocationHint {
 public:
  KeySSTZoneAllocationHint() = default;
  KeySSTZoneAllocationHint(size_t size, int level)
      : size_(size), level_(level) {}
  size_t size_;
  int level_;
};

class Zone {
  ZonedBlockDevice *zbd_;
  std::atomic_bool busy_;

 public:
  explicit Zone(ZonedBlockDevice *zbd, struct zbd_zone *z);

  uint64_t start_;
  uint64_t capacity_; /* remaining capacity */
  uint64_t max_capacity_;
  uint64_t wp_;
  Env::WriteLifeTimeHint lifetime_;
  std::atomic<uint64_t> used_capacity_;

  // Mark if the zone is in provisioning state, can be mutate only
  // after calling Acquire on it
  bool provisioning_zone_;

  IOStatus Reset();
  IOStatus Finish();
  IOStatus Close();

  IOStatus Append(char *data, uint32_t size, bool is_gc = false);
  bool IsUsed();
  bool IsFull();
  bool IsEmpty();
  uint64_t GetZoneNr();
  uint64_t GetCapacityLeft();
  bool IsBusy() const { return this->busy_.load(std::memory_order_relaxed); }
  bool Acquire() {
    bool expected = false;
    return this->busy_.compare_exchange_strong(expected, true,
                                               std::memory_order_acq_rel);
  }
  bool Release() {
    bool expected = true;
    return this->busy_.compare_exchange_strong(expected, false,
                                               std::memory_order_acq_rel);
  }

  void LoopForAcquire() {
    while (!Acquire())
      ;
  }

  void SetProvisioningFlag(bool flag) { provisioning_zone_ = flag; }

  bool IsProvisioningZone() const { return provisioning_zone_; }

  void EncodeJson(std::ostream &json_stream);

  inline IOStatus CheckRelease();

  // (kqh): return the id of zone for readability
  int ZoneId() const;
  // (kqh): Dump the status of current zone
  std::string ToString() const;
};

/*
 * class KeySSTZones
 * KeySSTZones manages *ALL* zones used for key SST.
 */
class KeySSTZones {
 private:
  // these zones store levels from Level 0 to Level k
  // where k is currently 3
  std::vector<Zone *> lower_zones_;
  std::vector<Zone *> high_zones_;

 public:
  KeySSTZones() = default;
  ~KeySSTZones() = default;

  void PushBackZone(Zone *zone, bool to_lower = true) {
    if (to_lower) {
      lower_zones_.push_back(zone);
    } else {
      high_zones_.push_back(zone);
    }
  }

  std::vector<Zone *> &LowZones() { return lower_zones_; }

  std::vector<Zone *> &HighZones() { return high_zones_; }
};

class ValueSSTZones {};

class ZonedBlockDevice {
 private:
  // (xzw:TODO) considering referecing the ZenFS within zbd_
  ZenFS* zenfs_ = nullptr;

  std::string filename_;
  uint32_t block_sz_;
  uint64_t zone_sz_;
  uint32_t nr_zones_;
  // The number requirement for over-provisioning zones, it depends on the
  // number of total zones.
  uint32_t nr_provisioning_zones_;
  std::vector<Zone *> io_zones;

  // [kqh] More specific zone allocation
  std::vector<Zone *> wal_zones_;

  int active_wal_zone_ = kNoActiveWALZone;
  const static int kNoActiveWALZone = -1;

  std::vector<Zone *> key_sst_zones_;
  int active_aggr_keysst_zone_ = kNoActiveAggrKeySSTZone;
  const static int kNoActiveAggrKeySSTZone = -1;

  std::vector<Zone *> value_sst_zones_;

  std::vector<Zone *> meta_zones;
  // Zones for over-provisioning
  std::vector<Zone *> provisioning_zones;
  int read_f_;
  int read_direct_f_;
  int write_f_;
  time_t start_time_;
  std::shared_ptr<Logger> logger_;
  uint32_t finish_threshold_ = 0;

  std::atomic<long> active_io_zones_;
  std::atomic<long> open_io_zones_;
  /* Protects zone_resuorces_  condition variable, used
     for notifying changes in open_io_zones_ */
  std::mutex zone_resources_mtx_;
  std::condition_variable zone_resources_;
  std::mutex zone_deferred_status_mutex_;
  IOStatus zone_deferred_status_;

  std::condition_variable migrate_resource_;
  std::mutex migrate_zone_mtx_;
  std::atomic<bool> migrating_{false};

  unsigned int max_nr_active_io_zones_;
  unsigned int max_nr_open_io_zones_;

  // Some metrics
  std::shared_ptr<ZenFSMetrics> metrics_;
  std::shared_ptr<XZenFSMetrics> x_metrics_ = std::make_shared<XZenFSMetrics>();

  void EncodeJsonZone(std::ostream &json_stream,
                      const std::vector<Zone *> zones);

 public:
  explicit ZonedBlockDevice(std::string bdevname,
                            std::shared_ptr<Logger> logger,
                            std::shared_ptr<ZenFSMetrics> metrics =
                                std::make_shared<NoZenFSMetrics>());
  virtual ~ZonedBlockDevice();

  IOStatus Open(bool readonly, bool exclusive);
  IOStatus CheckScheduler();

  Zone *GetIOZone(uint64_t offset);

  IOStatus AllocateIOZone(Env::WriteLifeTimeHint file_lifetime, IOType io_type,
                          Zone **out_zone);
  IOStatus AllocateMetaZone(Zone **out_meta_zone);

  IOStatus AllocateWALZone(Zone **out_zone, WALZoneAllocationHint *hint);
  // [kqh] Switch current active WAL Zone
  IOStatus SwitchWALZone();
  // [kqh] If one WAL zone is not used, reset it
  IOStatus ResetUnusedWALZones();

  /*
   * (xzw:TODO): allocate a key sst from managed KeySSTZones
   *
   * During the allocation, if the active low zone is used up,
   * we should migrate valid files from the used-up zone to the backup
   * zone and activate the backup zone as the new active low zone
   */
  IOStatus AllocateKeySSTZone(Zone **out_zone, KeySSTZoneAllocationHint *hint);

  IOStatus AllocateValueSSTZone(Zone **out_zone,
                                ValueSSTZoneAllocationHint *hint);
  
  IOStatus ResetUnusedKeySSTZones();

  int GetAdvancedActiveWALZoneIndex() {
    return (active_wal_zone_ + 1) % wal_zones_.size();
  }

  long OpenIOZoneCount() const { return open_io_zones_.load(); }

  Zone *GetProvisioningZone() {
    if (provisioning_zones.empty()) {
      return nullptr;
    }
    auto ret_zone = provisioning_zones.back();
    provisioning_zones.pop_back();
    return ret_zone;
  }

  void AddProvisioningZone(Zone *z) { provisioning_zones.push_back(z); }

  uint64_t GetFreeSpace();
  uint64_t GetUsedSpace();
  uint64_t GetReclaimableSpace();

  std::string GetFilename();
  uint32_t GetBlockSize();

  IOStatus ResetUnusedIOZones();
  void LogZoneStats();
  void LogZoneUsage();
  void LogGarbageInfo();

  int GetReadFD() { return read_f_; }
  int GetReadDirectFD() { return read_direct_f_; }
  int GetWriteFD() { return write_f_; }

  uint64_t GetZoneSize() { return zone_sz_; }
  uint32_t GetNrZones() { return nr_zones_; }
  std::vector<Zone *> GetMetaZones() { return meta_zones; }

  void SetFinishTreshold(uint32_t threshold) { finish_threshold_ = threshold; }

  void PutOpenIOZoneToken();
  void PutActiveIOZoneToken();

  /*
  bool HasEnoughProvisioningZones() const {
    return provisioning_zones.size() >= nr_provisioning_zones_;
  }
  */

  void EncodeJson(std::ostream &json_stream);

  void SetZoneDeferredStatus(IOStatus status);

  std::shared_ptr<ZenFSMetrics> GetMetrics() { return metrics_; }
  std::shared_ptr<XZenFSMetrics> GetXMetrics() { return x_metrics_; }

  void GetZoneSnapshot(std::vector<ZoneSnapshot> &snapshot);

  int DirectRead(char *buf, uint64_t offset, int n);

  IOStatus ReleaseMigrateZone(Zone *zone);

  IOStatus TakeMigrateZone(Zone **out_zone, Env::WriteLifeTimeHint lifetime,
                           uint32_t min_capacity);

  void GetZonesForWALAllocation() {
    // Always use the first two zones for WAL allocation
    wal_zones_.push_back(io_zones[0]);
    wal_zones_.push_back(io_zones[1]);
  }

  void GetZonesForKeySSTAllocation() {
    // Always use 3 zones preceding wal zones for KeySST allocation
    // io_zones[2]->capacity_ /= 100;
    // io_zones[3]->capacity_ /= 100;
    // io_zones[4]->capacity_ /= 100;
    key_sst_zones_.push_back(io_zones[2]);
    key_sst_zones_.push_back(io_zones[3]);
    key_sst_zones_.push_back(io_zones[4]);

    // (xzw): should restore the correct active zone
    // (kqh): Continue writing the zone that has been written already
    active_aggr_keysst_zone_ = io_zones[2]->IsEmpty() ? 1 : 0;
  }

  IOStatus MigrateAggregatedLevelZone();

  bool IsSpecialZone(const Zone *zone) {
    bool is_wal_zone = zone->ZoneId() == 3 || zone->ZoneId() == 4;
    bool is_keysst_zone =
        zone->ZoneId() == 5 || zone->ZoneId() == 6 || zone->ZoneId() == 7;
    return is_wal_zone || is_keysst_zone;
  }

  bool IsAggregatedLevel(int level) {
    return level >= 0 && level <= config::kAggrLevelThreshold;
  }

 public:
  std::string ErrorToString(int err);
  IOStatus GetZoneDeferredStatus();
  bool GetActiveIOZoneTokenIfAvailable();
  void WaitForOpenIOZoneToken(bool prioritized);
  IOStatus ApplyFinishThreshold();
  IOStatus FinishCheapestIOZone();
  IOStatus GetBestOpenZoneMatch(Env::WriteLifeTimeHint file_lifetime,
                                unsigned int *best_diff_out, Zone **zone_out,
                                uint32_t min_capacity = 0);
  IOStatus AllocateEmptyZone(Zone **zone_out);

  void SetZenFS(ZenFS* fs) { zenfs_ = fs; }
  ZenFS* GetZenFS() const { return zenfs_; }
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
