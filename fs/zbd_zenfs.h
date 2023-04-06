// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <queue>
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
#include <deque>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "libcuckoo/cuckoohash_map.hh"
#include "log.h"
#include "metrics.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"

namespace ROCKSDB_NAMESPACE {

using zone_id_t = uint64_t;
static const zone_id_t kInvalidZoneId = -1;

using libcuckoo::cuckoohash_map;

namespace config {
const static int kAggrLevelThreshold = 4;
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
  KeySSTZoneAllocationHint(size_t size, int level, const std::string &filename)
      : size_(size), level_(level), filename_(filename) {}
  size_t size_;
  int level_;
  std::string filename_;
};

// Describe A Zone's stats in the view of GC
// We can keep ZoneGCStats in several heaps for quickly finding the zone with
// the highest GC ratio
//
// Dynamically sorting the stats may not be desirable because it takes hundres
// of microseconds, meanwhile, read/write takes only tens of microseconds
struct ZoneGCStats {
  ZoneGCStats(zone_id_t id)
      : zone_id(id), no_blobs(0), no_kv(1), no_valid_kv(0), wasted_size(0) {}

  ZoneGCStats() : ZoneGCStats(kInvalidZoneId) {}
  ~ZoneGCStats() = default;

  // we keep zone GCStats away from Zones, so that we can sort these stats
  // in various ways as we need
  zone_id_t zone_id;

  // Make this field atomic for a guarantee of thread-safety
  std::atomic<uint64_t> no_blobs;
  std::atomic<uint64_t> no_kv;  // avoid float exception
  std::atomic<uint64_t> no_valid_kv;
  std::atomic<uint64_t> wasted_size;  // space unused when Finish()
  // number of records deprecated by other records placed in the same zone
  std::atomic<uint64_t> in_zone_deprecated;

  // A Clear() interface reset the recorded states of the zone. It is used
  // when a gc task is finished
  void Clear() {
    no_blobs = 0;
    no_kv = 0;
    no_valid_kv = 0;
    wasted_size = 0;
    in_zone_deprecated = 0;
  }

  std::string Dump() {
    char buf[512];
    sprintf(buf,
            "[Zone: %lu][Wasted: %.2lf MiB][NoBlob: %lu][NoKV: %lu][NoValidKV: "
            "%lu][GR: %.2lf][InZoneGR: %.2lf]",
            zone_id, ToMiB(wasted_size.load()), no_blobs.load(), no_kv.load(),
            no_valid_kv.load(), GarbageRatio(), InZoneGarbageRatio());
    return std::string(buf);
  }

  double GarbageRatio() const {
    return no_kv == 0 ? 0 : 1 - (double)no_valid_kv / no_kv;
  }
  double InZoneGarbageRatio() const {
    return no_kv == 0 ? 0 : (double)in_zone_deprecated / no_kv;
  }
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
    // ZnsLog(kCyan, "Zone(%lu) LoopForAcquire::Start", ZoneId());
    while (!Acquire())
      ;
    // ZnsLog(kCyan, "Zone(%lu) LoopForAcquire::End", ZoneId());
  }

  void SetProvisioningFlag(bool flag) { provisioning_zone_ = flag; }

  bool IsProvisioningZone() const { return provisioning_zone_; }

  void EncodeJson(std::ostream &json_stream);

  IOStatus CheckRelease();

  // (kqh): return the id of zone for readability
  zone_id_t ZoneId() const;
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
  friend class ZenFS;
  static constexpr uint64_t kMaxPartitionNum = 32;

 private:
  // (xzw:TODO) considering referecing the ZenFS within zbd_
  ZenFS *zenfs_ = nullptr;

  std::string filename_;
  uint32_t block_sz_;
  uint64_t zone_sz_;
  uint32_t nr_zones_;
  // The number requirement for over-provisioning zones, it depends on the
  // number of total zones.
  uint32_t nr_provisioning_zones_;
  std::vector<Zone *> io_zones;

  // ========================================================================
  // ZNS Project
  // ========================================================================

  // ------------------- Garbage Related Management ---------------------- //

  // ZoneDeprecationGraph is a strcture that depicts the deprecation
  // relationship between zones. An edge <<src, dst>, num> means Zone(src)
  // deprecates num records in Zone(dst) This structure is used to calculate a
  // "good" approximation of garbages across zones
  struct ZoneDeprecationGraph {
    using GraphNode = cuckoohash_map<zone_id_t, uint64_t>;

    uint64_t zone_num;
    std::vector<std::shared_ptr<GraphNode>> graphs;

    ZoneDeprecationGraph() = default;
    ~ZoneDeprecationGraph() = default;

    // This can be only called once
    void Init(uint32_t _zone_num) {
      zone_num = _zone_num;
      graphs.assign(_zone_num, nullptr);
    }

    void AddEdgeWeight(zone_id_t src, zone_id_t dst, uint64_t num) {
      if (graphs[src] == nullptr) {
        graphs[src] = std::make_shared<GraphNode>();
      }
      if (!graphs[src]->update_fn(dst, [&](auto &d) { d += num; })) {
        graphs[src]->insert(dst, num);
      }
    }

    void Clear(zone_id_t src, zone_id_t dst) {
      auto p = graphs[src];
      p->update_fn(dst, [](auto &d) { d = 0; });
    }

    uint64_t GetEdgeWeight(zone_id_t src, zone_id_t dst) {
      auto p = graphs[src];
      uint64_t ret = 0;
      return p->find(dst, ret) ? ret : 0;
    }
  };

  // maps from zone ID to zone GC stats
  std::unordered_map<zone_id_t, ZoneGCStats> zone_gc_stats_map_;
  ZoneDeprecationGraph zone_graphs_;

  // --------------------------------------------------------------------- //

  // [kqh] More specific zone allocation
  std::vector<Zone *> wal_zones_;

  int active_wal_zone_ = kNoActiveWALZone;
  const static int kNoActiveWALZone = -1;

  // Key SST Zone management related data structures
  std::vector<Zone *> key_sst_zones_;
  int active_aggr_keysst_zone_ = kNoActiveAggrKeySSTZone;
  const static int kNoActiveAggrKeySSTZone = -1;

  // Value SST Zone management related data structures
  std::vector<Zone *> value_sst_zones_;

  // May use an implementation of concurrent queue.
  std::deque<zone_id_t> empty_zones_;
  std::mutex empty_zone_mtx_;

 public:
  // ------------------------- Value SST Management --------------------------
  //
  // A ZonePartition is an abstraction and calpulation of zones in a specific
  // partition: HashPartition/Hot/Warm.
  //
  // In normal case, each partition contains only one activated zone absorbing
  // incoming SST write. When GC occurs, addtitional zones would be added and
  // used for GC write to avoid intervening foreground write
  //
  // This struct only provides basic functionalities. For Hot/Warm partition,
  // implement a specific class derived from this struct and add specialized
  // interfaces.
  //
  // This struct should be implemented as thread-safe structs
  struct ZonePartition {
    std::unordered_set<zone_id_t> zones;
    zone_id_t activated_zone = kInvalidZoneId;
    ZonedBlockDevice *zbd;

    //
    // The current zone being written for garbage collected data. We consider
    // decomposing the activated_zone and gc_write_zone. The formmer is used
    // for absorbing forground write while the latter is used for aborbing
    // data of resultant file of a garbage collection scheme. Decomposing
    // zones of two different usage helps preventing intervening.
    //
    // However, designating an extra gc_write_zone requires extra active and
    // open token. We consider reusing the activated_zone in the presence of
    // tokens' inadequacy.
    //
    zone_id_t curr_gc_write_zone = kInvalidZoneId;
    uint64_t gc_write_zone_wasted = 0;
    uint64_t gc_reclaim = 0;
    uint64_t reset_count = 0;

    bool HasGCWriteZone() const { return curr_gc_write_zone != kInvalidZoneId; }

    //
    // A zone used for garbage collection can not be reset immediately after
    // garbage collection is done successfully. There might be multiple readers
    // reference files located in this zone.
    //
    // We use a pending queue to delay the actual reset operation of logically
    // reclaimed zones until all files in this zone has been deleted.
    //
    // The second field of the element in pending_reset_zones marks the
    // timepoint this zone is added into this queue. It will be used to
    // calculate the time interval between its adding and reset
    struct PendingZoneStats {
      zone_id_t z_id;
      uint64_t add_time;
      uint64_t gc_write_bytes; // number of bytes written gc this zone
    };
    std::deque<PendingZoneStats> pending_reset_zones;
    std::mutex pending_reset_zone_mtx;  // For concurrent control

    ZonePartition(const std::unordered_set<zone_id_t> &_zones,
                  ZonedBlockDevice *_zbd)
        : zones(_zones), activated_zone(kInvalidZoneId), zbd(_zbd) {}

    // Getter and Setter
    zone_id_t GetActivatedZone() const { return activated_zone; }
    void SetActivateZone(zone_id_t zone) { activated_zone = zone; }
    zone_id_t GetCurrGCWriteZone() const { return curr_gc_write_zone; }
    void SetCurrGCWriteZone(zone_id_t zone) { curr_gc_write_zone = zone; }

    void AddPendingResetZone(zone_id_t zone, uint64_t bytes) {
      std::scoped_lock<std::mutex> lck(pending_reset_zone_mtx);
      pending_reset_zones.push_back({zone, Env::Default()->NowMicros(), bytes});
    }

    // Manipulate interface for contained zones in this partition
    void AddZone(zone_id_t zone) {
      assert(zones.count(zone) == 0);
      zones.emplace(zone);
    }

    void RemoveZone(zone_id_t zone) {
      assert(zones.count(zone) == 1);
      zones.erase(zone);
    }

    std::unordered_set<zone_id_t> GetZones() const { return zones; }

    void MaybeResetPendingZones() {
      // ZnsLog(kCyan, "MaybeResetPendingZones: Start");
      std::scoped_lock<std::mutex> lck(pending_reset_zone_mtx);
      auto reset_zone =
          [&](const auto &elem) -> bool {
        auto z_id = elem.z_id;
        auto zone = zbd->GetZone(z_id);
        assert(zone->IsFull());
        zone->LoopForAcquire();
        if (zone->used_capacity_ == 0) {
          // If Reset() fails, keeping checking this zone during
          // next round.
          auto s = zone->Reset();
          zone->CheckRelease();
          if (!s.ok()) {
            return false;
          }
          uint64_t reset_end = Env::Default()->NowMicros();
          ZnsLog(kCyan,
                 "Partition() MaybeResetPendingZone: Zone%lu is reset (pass: "
                 "%lu us)",
                 zone->ZoneId(), reset_end - elem.add_time);
          // Record the bytes reclaimed by GC: it releases bytes of zone size 
          // but incurs "gc_write_bytes" write. 
          gc_reclaim += (zbd->GetZoneSize() - elem.gc_write_bytes);
          reset_count += 1;
          zbd->PushEmptyZone(z_id);
          // Clear the GC stats as well:
          auto gc_stat = zbd->GetZoneGCStatsOf(z_id);
          gc_stat->Clear();

          return true;
        }
        zone->CheckRelease();
        return false;
      };
      // Note that remove_if does not do the actual element erase operation
      auto iter = std::remove_if(pending_reset_zones.begin(),
                                 pending_reset_zones.end(), reset_zone);
      pending_reset_zones.erase(iter, pending_reset_zones.end());
    }

    // For Debug purpose
    std::string Dump() {
      std::stringstream ss;
      for (const auto &zone_id : zones) {
        auto zone = zbd->GetZone(zone_id);
        auto gc_stat = zbd->GetZoneGCStatsOf(zone_id);
        ss << "[Zone" << zone_id
           << "][Capacity: " << ToMiB(zone->GetCapacityLeft()) << "MiB]";
        ss << gc_stat->Dump() << "\n";
      }
      ss << "[GC Reclaim: " << ToMiB(gc_reclaim) << "MiB]" << "\n";
      ss << "[ResetCount: " << reset_count << "]\n";
      return ss.str();
    }

    // Finish curr_gc_write_zone and set curr_gc_write_zone to be invalid
    // Acquire: Current thread hold the lock of this zone
    IOStatus FinishCurrGCWriteZone() {
      IOStatus s = IOStatus::OK();
      if (curr_gc_write_zone == kInvalidZoneId) {
        return s;
      }
      auto gc_write_zone = zbd->GetZone(curr_gc_write_zone);
      bool full = gc_write_zone->IsFull();
      gc_write_zone_wasted += gc_write_zone->GetCapacityLeft();
      zbd->GetZoneGCStatsOf(curr_gc_write_zone)->wasted_size +=
          gc_write_zone->GetCapacityLeft();
      // Finish this zone when it has no enough space
      s = gc_write_zone->Finish();
      if (!s.ok()) {
        return s;
      }
      s = gc_write_zone->Close();
      gc_write_zone->CheckRelease();
      if (!s.ok()) {
        return s;
      }
      curr_gc_write_zone = kInvalidZoneId;
      return s;
    }
  };

  struct HashPartition : public ZonePartition {
    HashPartition(const std::unordered_set<zone_id_t> &_zones,
                  ZonedBlockDevice *_zbd)
        : ZonePartition(_zones, _zbd) {}
  };
  struct HotPartition : public ZonePartition {
    HotPartition(const std::unordered_set<zone_id_t> &_zones,
                 ZonedBlockDevice *_zbd)
        : ZonePartition(_zones, _zbd) {}
  };

  struct PartitionGCHist {
    uint64_t hist_bit_map[kMaxPartitionNum] = {0};

    void MarkGC(uint64_t partition) {
      for (uint64_t i = 0; i < 4; ++i) {
        hist_bit_map[i] <<= 1;
      }
      hist_bit_map[partition] |= 0x01;
    }

    int LatestGCCount(uint64_t partition) {
      return __builtin_ctzl(~hist_bit_map[partition]);
    }
  };

  // This field should be initialized from the constructor parameters.
  uint32_t partition_num = 4;
  std::shared_ptr<HashPartition> hash_partitions_[kMaxPartitionNum];
  std::shared_ptr<HotPartition> hot_partition_;
  std::shared_ptr<HotPartition> warm_partition_;
  std::shared_ptr<PartitionGCHist> gc_hist_;

  // --------------------------------------------------------------------------

  // ========================================================================

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
  /* Protects zone_resources_  condition variable, used
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
  uint64_t last_gc_finish_time_ = 0;

  // A map recording Blob SST (file number) stored in a specific zone
  // NOTE: Initialized from persistent metadata. We place this struct in zbd
  // instead of ZenFS since ZoneFile can directly access zbd.
  // For simplicity we use an array to store this map and use zone id for
  // indexing.
  std::vector<uint64_t> zone_2_file_[300];

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

  // Add a file number to zone
  void AddFileToZone(uint64_t f, zone_id_t zone_id) {
    auto &z = zone_2_file_[zone_id];
    assert(std::find(z.begin(), z.end(), f) == z.end());
    z.push_back(f);
  }

  // This interface should be invoked when a specific file is deleted.
  void RemoveFileFromZone(uint64_t f, zone_id_t zone_id) {
    auto &z = zone_2_file_[zone_id];
    auto it = std::find(z.begin(), z.end(), f);
    assert(it != z.end());
    z.erase(it);
  }

  std::unordered_set<uint64_t> GetFilesOfZone(zone_id_t zone_id) {
    std::unordered_set<uint64_t> ret;
    for (const auto &fn : zone_2_file_[zone_id]) {
      ret.insert(fn);
    }
    return ret;
  }

  std::vector<uint64_t> GetFilesOfZoneAsVec(zone_id_t zone_id) {
    std::vector<uint64_t> ret;
    for (const auto &fn : zone_2_file_[zone_id]) {
      ret.emplace_back(fn);
    }
    return ret;
  }

  Zone *GetIOZone(uint64_t offset);

  IOStatus AllocateIOZone(Env::WriteLifeTimeHint file_lifetime, IOType io_type,
                          Zone **out_zone);
  IOStatus AllocateMetaZone(Zone **out_meta_zone);

  // Some monitoring information
  std::atomic<int> wal_zone_open_count_;
  std::atomic<int> wal_zone_active_count_;

  std::atomic<int> keysst_aggr_zone_open_count_;
  std::atomic<int> keysst_aggr_zone_active_count_;

  // Alnert: This field might need persistence and should be initialized from
  // ZenFS log.
  std::atomic<bool> is_disaggr_zone_written_;

  std::atomic<int> keysst_disaggr_zone_open_count_;
  std::atomic<int> keysst_disaggr_zone_active_count_;

  // [kqh] If one WAL zone is not used, reset it
  IOStatus ResetUnusedWALZones();

  IOStatus AllocateValueSSTZone(Zone **out_zone,
                                ValueSSTZoneAllocationHint *hint);

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
  // (kqh): OccupySpace means the space that has been written, i.e. The space
  // between start space and write pointer of each zone
  uint64_t GetOccupySpace();
  double GetPartitionGR();
  double GetPartitionGR(HotnessType type);

  std::string GetFilename();
  uint32_t GetBlockSize();

  ZoneGCStats *GetZoneGCStatsOf(uint64_t zone_id) {
    auto it = zone_gc_stats_map_.find(zone_id);
    assert(zone_id < nr_zones_);
    assert(it != zone_gc_stats_map_.end());
    return &it->second;
  }

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

  // ===========================================================
  // =========== Bytedance Project Development ==================
  // ===========================================================

  // -------- Initialization related functions ---------------//

  void InitWALZones();

  void InitKeySSTZones();

  // Enqueue all empty zones except for special zones
  // (i.e. Meta Zone, WAL Zone, KeySST Zone)
  void InitEmptyZoneQueue();

  // -------- KeySST Zone Allocation related functions ---------------//

  IOStatus AllocateKeySSTZone(Zone **out_zone, KeySSTZoneAllocationHint *hint);
  IOStatus ResetUnusedKeySSTZones();
  // ------------------------------------------------------------------

  // ------------ WAL Zone Allocation related functions ---------------//
  IOStatus AllocateWALZone(Zone **out_zone, WALZoneAllocationHint *hint);

  // [kqh] Switch current active WAL Zone
  IOStatus SwitchWALZone();
  // ------------------------------------------------------------------//

  // Get one empty zone from empty zone queue. This function is thread-safe.
  // Return nullptr if no empty zone is available
  zone_id_t PopEmptyZone();

  // Push a zone into the empty zone queue. This function is thread-safe
  // Abort if the target zone is already in empty queue
  void PushEmptyZone(zone_id_t zone);

  // Get the zone struct using according zone id.
  Zone *GetZone(zone_id_t zone_id);

  // Get the proper partition
  std::shared_ptr<ZonePartition> GetPartition(HotnessType type) {
    if (type.IsHot())
      return hot_partition_;
    else if (type.IsWarm())
      return warm_partition_;
    else if (type.IsPartition())
      return hash_partitions_[type.PartitionId()];
    else
      return nullptr;
  }

  void InitializeZoneGCStats();

  void ZnsLogKeySSTZoneToken() {
    ZnsLog(kMagenta,
           "KeySSTAggrZoneOpenCount(%d) KeySSTAggrZoneActiveCount(%d) "
           "KeySSTDisAggrZoneOpenCount(%d) KeySSTDisAggrZoneActiveCount(%d)",
           keysst_aggr_zone_open_count_.load(),
           keysst_aggr_zone_active_count_.load(),
           keysst_disaggr_zone_open_count_.load(),
           keysst_disaggr_zone_active_count_.load());
  }

  IOStatus MigrateAggregatedLevelZone();

  // ----------------------- Value SST Management ------------------------- //

  // Initialize the states of each partition (Hot/Warm partition, hash
  // partition). For current implementation, each partition is initialized
  // assuming the ZonedBlockDevice is in a bare state. However, the zone states
  // of each partition needs to be persist and the implementation of this
  // initialization reads these persistent records
  void InitializePartitions();

  // Pick one zone with highest garbage ratio in a specific partition. Return
  // Invalid Zone id if no appropriate zone can be returned
  std::pair<zone_id_t, double> PickZoneWithHighestGarbageRatio(
      const ZonePartition *partition);
  std::pair<zone_id_t, double> PickZoneFromHotPartition() {
    return PickZoneWithHighestGarbageRatio(hot_partition_.get());
  }
  std::pair<zone_id_t, double> PickZoneFromWarmPartition() {
    return PickZoneWithHighestGarbageRatio(warm_partition_.get());
  }

  std::pair<zone_id_t, double> PickZoneFromHashPartition(
      uint32_t *partition_id);

  std::pair<zone_id_t, double> MaybeSwitchToAnotherPartition(
      uint32_t partition, uint32_t *choose_partition);

  std::unordered_set<uint64_t> GetFilesFromZone(zone_id_t zone_id);

  // ===========================================================
  // =========== Bytedance Project Development (END) ==================
  // ===========================================================

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

  bool IsSpecialZone(const Zone *zone) {
    bool is_wal_zone = zone->ZoneId() == 3 || zone->ZoneId() == 4;
    bool is_keysst_zone = zone->ZoneId() >= 5 && zone->ZoneId() <= 8;
    return is_wal_zone || is_keysst_zone;
  }

  bool IsValueSSTZone(const Zone *zone) {
    return zone->ZoneId() >= 9 && zone->ZoneId() <= 99;
    return false;
  }

  static bool IsAggregatedLevel(int level) {
    return level >= 0 && level <= config::kAggrLevelThreshold;
  }

  void SetZenFS(ZenFS *fs) { zenfs_ = fs; }
  ZenFS *GetZenFS() const { return zenfs_; }
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
