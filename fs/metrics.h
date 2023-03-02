// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

// Metrics Framework Introduction
//
// The metrics framework is used for users to identify ZenFS's performance
// bottomneck, it can collect throuput, qps and latency of each critical
// function call.
//
// For different RocksDB forks, users could custom their own metrics reporter to
// define how they would like to report these collected information.
//
// Steps to add new metrics trace point:
//   1. Add a new trace point label name in `ZenFSMetricsHistograms`.
//   2. Find target function, add these lines for tracing
//       // Latency Trace
//       ZenFSMetricsGuard guard(zoneFile_->GetZBDMetrics(),
//       ZENFS_WAL_WRITE_LATENCY, Env::Default());
//       // Throughput Trace
//       zoneFile_->GetZBDMetrics()->ReportThroughput(ZENFS_WRITE_THROUGHPUT,
//       data.size());
//       // QPS Trace
//       zoneFile_->GetZBDMetrics()->ReportQPS(ZENFS_WRITE_QPS, 1);
//    3. Implement a `ZenFSMetrics` to define how you would like to report your
//    data (Refer to file  `metrics_sample.h`)
//    4. Define your customized label name when implement ZenFSMetrics
//    5. Init your metrics and pass it into `NewZenFS()` function (default is
//    `NoZenFSMetrics`)

#pragma once
#include <algorithm>
#include <atomic>
#include <chrono>
#include <fstream>
#include <sstream>

#include "log.h"
#include "rocksdb/env.h"
namespace ROCKSDB_NAMESPACE {

class ZenFSMetricsGuard;
class ZenFSSnapshot;
class ZenFSSnapshotOptions;

// Types of Reporter that may be used for statistics.
enum ZenFSMetricsReporterType : uint32_t {
  ZENFS_REPORTER_TYPE_WITHOUT_CHECK = 0,
  ZENFS_REPORTER_TYPE_GENERAL,
  ZENFS_REPORTER_TYPE_LATENCY,
  ZENFS_REPORTER_TYPE_QPS,
  ZENFS_REPORTER_TYPE_THROUGHPUT,
};

// Names of Reporter that may be used for statistics.
enum ZenFSMetricsHistograms : uint32_t {
  ZENFS_HISTOGRAM_ENUM_MIN,

  ZENFS_READ_LATENCY,
  ZENFS_READ_QPS,

  ZENFS_WRITE_LATENCY,
  ZENFS_WAL_WRITE_LATENCY,
  ZENFS_NON_WAL_WRITE_LATENCY,
  ZENFS_WRITE_QPS,
  ZENFS_WRITE_THROUGHPUT,

  ZENFS_SYNC_LATENCY,
  ZENFS_WAL_SYNC_LATENCY,
  ZENFS_NON_WAL_SYNC_LATENCY,
  ZENFS_SYNC_QPS,

  ZENFS_IO_ALLOC_LATENCY,
  ZENFS_WAL_IO_ALLOC_LATENCY,
  ZENFS_NON_WAL_IO_ALLOC_LATENCY,
  ZENFS_IO_ALLOC_QPS,

  ZENFS_META_ALLOC_LATENCY,
  ZENFS_META_ALLOC_QPS,

  ZENFS_META_SYNC_LATENCY,

  ZENFS_ROLL_LATENCY,
  ZENFS_ROLL_QPS,
  ZENFS_ROLL_THROUGHPUT,

  ZENFS_ACTIVE_ZONES_COUNT,
  ZENFS_OPEN_ZONES_COUNT,

  ZENFS_FREE_SPACE_SIZE,
  ZENFS_USED_SPACE_SIZE,
  ZENFS_RECLAIMABLE_SPACE_SIZE,

  ZENFS_RESETABLE_ZONES_COUNT,

  ZENFS_HISTOGRAM_ENUM_MAX,

  ZENFS_ZONE_WRITE_THROUGHPUT,
  ZENFS_ZONE_WRITE_LATENCY,

  ZENFS_ZONE_GC_WRITE_THROUGHPUT,
  ZENFS_ZONE_GC_WRITE_LATENCY,

  ZENFS_ZONE_WAL_WRITE_THROUGHPUT,

  ZENFS_ZONE_FLUSH_FILE_WRITE_THROUGHPUT,

  ZENFS_ZONE_COMPACTION_OUTPUT_FILE_WRITE_THROUGHPUT,

  ZENFS_ZONE_READ_THROUGHPUT,

  ZENFS_ZONE_GC_READ_THROUGHPUT,

  ZENFS_L0_IO_ALLOC_LATENCY,
};

struct ZenFSMetrics {
 public:
  typedef uint32_t Label;
  typedef uint32_t ReporterType;
  // We give an enum to identify the reporters and an enum to identify the
  // reporter types: ZenFSMetricsHistograms and ZenFSMetricsReporterType,
  // respectively, at the end of the code.
 public:
  ZenFSMetrics() {}
  virtual ~ZenFSMetrics() {}

 public:
  // Add a reporter named label.
  // You can give a type for type-checking.
  virtual void AddReporter(Label label, ReporterType type = 0) = 0;
  // Report a value for the reporter named label.
  // You can give a type for type-checking.
  virtual void Report(Label label, size_t value,
                      ReporterType type_check = 0) = 0;
  virtual void ReportSnapshot(const ZenFSSnapshot& snapshot) = 0;

 public:
  // Syntactic sugars for type-checking.
  // Overwrite them if you think type-checking is necessary.
  virtual void ReportQPS(Label label, size_t qps) { Report(label, qps, 0); }
  virtual void ReportThroughput(Label label, size_t throughput) {
    Report(label, throughput, 0);
  }
  virtual void ReportLatency(Label label, size_t latency) {
    Report(label, latency, 0);
  }
  virtual void ReportGeneral(Label label, size_t data) {
    Report(label, data, 0);
  }

  // and more
};

struct NoZenFSMetrics : public ZenFSMetrics {
  NoZenFSMetrics() : ZenFSMetrics() {}
  virtual ~NoZenFSMetrics() {}

 public:
  virtual void AddReporter(uint32_t /*label*/, uint32_t /*type*/) override {}
  virtual void Report(uint32_t /*label*/, size_t /*value*/,
                      uint32_t /*type_check*/) override {}
  virtual void ReportSnapshot(const ZenFSSnapshot& /*snapshot*/) override {}
};

// The implementation of this class will start timing when initialized,
// stop timing when it is destructured,
// and report the difference in time to the target label via
// metrics->ReportLatency(). By default, the method to collect the time will be
// to call env->NowMicros().
struct ZenFSMetricsLatencyGuard {
  std::shared_ptr<ZenFSMetrics> metrics_;
  uint32_t label_;
  Env* env_;
  uint64_t begin_time_micro_;

  ZenFSMetricsLatencyGuard(std::shared_ptr<ZenFSMetrics> metrics,
                           uint32_t label, Env* env)
      : metrics_(metrics),
        label_(label),
        env_(env),
        begin_time_micro_(GetTime()) {}

  virtual ~ZenFSMetricsLatencyGuard() {
    uint64_t end_time_micro_ = GetTime();
    assert(end_time_micro_ >= begin_time_micro_);
    metrics_->ReportLatency(label_,
                            Report(end_time_micro_ - begin_time_micro_));
  }
  // overwrite this function if you wish to capture time by other methods.
  virtual uint64_t GetTime() { return env_->NowMicros(); }
  // overwrite this function if you do not intend to report delays measured in
  // microseconds.
  virtual uint64_t Report(uint64_t time) { return time; }
};

// ====================================================================
//  Our own metrics and related structs
//
// ====================================================================

enum XZenFSMetricsType {
  kTotal,
  kWAL,
  kKeySST,
  kValueSST,
  kGCMigrate,
  kGCWrite,

  kOccupySpace,
  kUsedSpace,
  kFreeSpace,
};

// A thread-safe metrics
struct XZenFSMetrics {
  static const uint64_t kRecordIntervalMs = 1000;
  static const uint64_t kMaxHistCount = 10000;

  // Related monitor histogram of ZNS write behaviour
  std::array<std::atomic<uint64_t>, kMaxHistCount> wal_write_bytes_hist;
  std::array<std::atomic<uint64_t>, kMaxHistCount> value_sst_write_bytes_hist;
  std::array<std::atomic<uint64_t>, kMaxHistCount> key_sst_write_bytes_hist;
  std::array<std::atomic<uint64_t>, kMaxHistCount>
      gc_migrate_sst_write_bytes_hist;
  std::array<std::atomic<uint64_t>, kMaxHistCount> gc_new_sst_write_bytes_hist;
  std::array<std::atomic<uint64_t>, kMaxHistCount> zns_write_bytes_hist;

  // Related monitors of ZNS space usage
  std::array<std::atomic<uint64_t>, kMaxHistCount> zns_occupy_space_hist;
  std::array<std::atomic<uint64_t>, kMaxHistCount> zns_used_space_hist;
  std::array<std::atomic<uint64_t>, kMaxHistCount> zns_free_space_hist;

  decltype(std::chrono::steady_clock::now()) start_time_point;

 public:
  XZenFSMetrics() : start_time_point(std::chrono::steady_clock::now()) {
    for (uint64_t i = 0; i < kMaxHistCount; ++i) {
      wal_write_bytes_hist[i].store(0);
      key_sst_write_bytes_hist[i].store(0);
      value_sst_write_bytes_hist[i].store(0);
      gc_migrate_sst_write_bytes_hist[i].store(0);
      gc_new_sst_write_bytes_hist[i].store(0);
      zns_write_bytes_hist[i].store(0);

      zns_occupy_space_hist[i].store(0);
      zns_used_space_hist[i].store(0);
      zns_free_space_hist[i].store(0);
    }
  }

  // Dump when destructed
  ~XZenFSMetrics() { Dump(""); }

  void RecordWriteBytes(uint64_t size, XZenFSMetricsType type) {
    auto elapse_time = ElapseTime();
    auto index = elapse_time / kRecordIntervalMs;
    switch (type) {
      case kWAL:
        wal_write_bytes_hist[index] += size;
        break;
      case kKeySST:
        key_sst_write_bytes_hist[index] += size;
        break;
      case kValueSST:
        value_sst_write_bytes_hist[index] += size;
        break;
      case kGCMigrate:
        gc_migrate_sst_write_bytes_hist[index] += size;
        break;
      case kGCWrite:
        gc_new_sst_write_bytes_hist[index] += size;
        break;
      case kTotal:
        zns_write_bytes_hist[index] += size;
        break;
      default:
        assert(false);
    }
  }

  void RecordZNSSpace(uint64_t size, XZenFSMetricsType type) {
    auto elapse_time = ElapseTime();
    auto index = elapse_time / kRecordIntervalMs;
    switch (type) {
      case kFreeSpace:
        zns_free_space_hist[index].store(size);
        break;
      case kOccupySpace:
        zns_occupy_space_hist[index].store(size);
        break;
      case kUsedSpace:
        zns_used_space_hist[index].store(size);
        break;
      default:
        assert(false);
    }
  }

  uint64_t ElapseTime() {
    auto time_now = std::chrono::steady_clock::now();
    auto dura = std::chrono::duration_cast<std::chrono::milliseconds>(
        time_now - start_time_point);
    return dura.count();
  }

  std::string SummarizeAsString() const {
    std::stringstream ss;
    ss << "[Key SST Write Bytes : " << ToMiB(0) << "MiB]"
       << "[Value SST Write Bytes :" << ToMiB(0) << "MiB]"
       << "[GC Migrate Write Bytes :" << ToMiB(0) << "MiB]"
       << "[ZNS Write Bytes : " << ToMiB(0) << "MiB]";
    return ss.str();
  }

  void Dump(const std::string& filename_prefix) {
    auto end_index = ElapseTime() / kRecordIntervalMs;

    // Dump write throughput
    DumpSingleHist(filename_prefix + "wal_write_bytes_hist",
                   wal_write_bytes_hist, end_index);
    DumpSingleHist(filename_prefix + "value_sst_write_bytes_hist",
                   value_sst_write_bytes_hist, end_index);
    DumpSingleHist(filename_prefix + "key_sst_write_bytes_hist",
                   key_sst_write_bytes_hist, end_index);
    DumpSingleHist(filename_prefix + "gc_migrate_sst_write_bytes_hist",
                   gc_migrate_sst_write_bytes_hist, end_index);
    DumpSingleHist(filename_prefix + "gc_new_sst_write_bytes_hist",
                   gc_new_sst_write_bytes_hist, end_index);
    DumpSingleHist(filename_prefix + "zns_write_bytes_hist",
                   zns_write_bytes_hist, end_index);

    // Dump space usage
    DumpSingleHist(filename_prefix + "zns_free_space_hist", zns_free_space_hist,
                   end_index);
    DumpSingleHist(filename_prefix + "zns_occupy_space_hist",
                   zns_occupy_space_hist, end_index);
    DumpSingleHist(filename_prefix + "zns_used_space_hist", zns_used_space_hist,
                   end_index);
  }

  void DumpSingleHist(
      const std::string& filename,
      const std::array<std::atomic<uint64_t>, kMaxHistCount>& data,
      uint64_t end_index, bool sum_up = true) {
    std::ofstream of;
    of.open(filename);

    // Calculate the total size
    if (sum_up) {
      uint64_t total = 0;
      std::for_each(data.begin(), data.end(),
                    [&](const auto& d) { total += d.load(); });
      of << total << "\n";
    }
    for (int i = 0; i < std::min(end_index, data.size()); ++i) {
      of << data[i] << "\n";
    }
    of.close();
  }
};

#define ZENFS_LABEL(label, type) ZENFS_##label##_##type
#define ZENFS_LABEL_DETAILED(label, sub_label, type) \
  ZENFS_##sub_label##_##label##_##type
// eg : ZENFS_LABEL(WRITE, WAL, THROUGHPUT) => ZENFS_WAL_WRITE_THROUGHPUT

}  // namespace ROCKSDB_NAMESPACE
