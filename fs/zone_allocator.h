#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <queue>
#include <set>
#include <sstream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "fs/fs_zenfs.h"
#include "fs/zbd_zenfs.h"
#include "libcuckoo/cuckoohash_map.hh"
#include "log.h"
#include "metrics.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"

namespace TERARKDB_NAMESPACE {
// An allocator managing empty zones to achieve wear leveling
// The system may acquire empty zones for write: WAL, KeySST and ValueSST.
// And they also return the zones that have been reset. This allocator
// gathers statistics about these zones and makes rational decisions
// when an allocating request arrives. This object is thread-safe.
class ZoneAllocator {
  struct ZoneStats {
    // Total time that have been reset
    uint64_t reset_count = 0;
  };

 public:
  ZoneAllocator(uint64_t zone_num)
      : zone_stats_(new ZoneStats[zone_num]()), min_heap_(less_op) {}
  ~ZoneAllocator() { delete[] zone_stats_; }



 private:
  ZoneStats* zone_stats_;
  std::function<bool(zone_id_t, zone_id_t)> less_op =
      [&](zone_id_t z1, zone_id_t z2) -> bool {
    return zone_stats_[z1].reset_count < zone_stats_[z2].reset_count;
  };
  std::priority_queue<zone_id_t, std::vector<zone_id_t>, decltype(less_op)>
      min_heap_;
  std::mutex mtx_;
};
}  // namespace TERARKDB_NAMESPACE