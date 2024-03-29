// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>

#include "fs/log.h"
#include "fs/metrics.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "util/filename.h"
#if !defined(ROCKSDB_LITE) && defined(OS_LINUX)

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "async.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"
#include "util.h"
#include "zbd_zenfs.h"

namespace ROCKSDB_NAMESPACE {

class ZoneExtent {
 public:
  uint64_t start_;
  uint64_t length_;
  Zone* zone_;

  explicit ZoneExtent(uint64_t start, uint64_t length, Zone* zone);
  Status DecodeFrom(Slice* input);
  void EncodeTo(std::string* output);
  void EncodeJson(std::ostream& json_stream);
};

class BufferedZoneExtent {
 public:
  BufferedZoneExtent(const ZoneExtent& zone_ext)
      : extent(zone_ext), buffered_data(){};
  ZoneExtent extent;
  Slice buffered_data;
};

class ZoneFile;

/* Interface for persisting metadata for files */
class MetadataWriter {
 public:
  virtual ~MetadataWriter();
  virtual IOStatus Persist(ZoneFile* zoneFile) = 0;
};

class ZoneFile {
 private:
  const uint64_t NO_EXTENT = 0xffffffffffffffff;
  static const int kMaxLSMLevel = 32;

  ZonedBlockDevice* zbd_;

  std::vector<ZoneExtent*> extents_;
  std::vector<std::string> linkfiles_;

  Zone* active_zone_;
  uint64_t extent_start_ = NO_EXTENT;
  uint64_t extent_filepos_ = 0;

  // (kqh & xzw): for TerarkDB on ZNS
  Env::WriteLifeTimeHint lifetime_;
  IOType io_type_; /* Only used when writing */
  uint64_t file_size_;
  uint64_t file_id_;
  uint64_t level_;

  // The placement hint passed above. Only valid when current file type is
  // ValueSST.
  PlacementFileType place_ftype_;

  // xzw: Since we do not allow files to span multiple zones
  // we use this field to track the zone that this file
  // is stored in.
  //
  // Questionable: not sure how the output files are determined
  Zone* belonged_zone_;

  uint32_t nr_synced_extents_ = 0;
  bool open_for_wr_ = false;
  time_t m_time_;
  bool is_sparse_ = false;
  bool is_deleted_ = false;

  MetadataWriter* metadata_writer_ = NULL;

  std::mutex writer_mtx_;
  std::atomic<int> readers_{0};

  struct FileAsyncReadRequest {
    // State of this request
    // The state transfer are as follows:
    // Idle ---------> Pending ----------> Finish ----------> Idle
    //      Submit()          CheckFinish()         Reset()
    enum State {
      kIdle,
      kPending,
      kFinish,
    };

    AsyncIORequest io_req;
    State state = kIdle;

    uint64_t file_req_offset;  // file offset of this request
    uint64_t file_req_sz;      // size of this file request
    uint64_t file_dev_offset;  // translated device offset
    uint64_t dev_req_offset;   // offset of request sent to device
    uint64_t dev_req_sz;       // size of request sent to device

    // The buffer for asynchronous read.
    char* async_buf_ = nullptr;
    size_t async_buf_size_ = 0;

    void Init() {
      file_req_offset = 0;
      file_req_sz = 0;
      file_dev_offset = 0;
      dev_req_offset = 0;
      dev_req_sz = 0;
      state = kIdle;
    }

    IOStatus Reset() {
      file_req_offset = 0;
      file_req_sz = 0;
      file_dev_offset = 0;
      dev_req_offset = 0;
      dev_req_sz = 0;

      // Set the io_req state accordingly: 
      if (IsPending()) {
        // Cancel the request that is being pending
        auto s = io_req.Cancel();
      }
      state = kIdle;
      return IOStatus::OK();
    }

    bool IsPending() const { return state == kPending; }
    void SetPending() { state = kPending; }

    bool IsIdle() const { return state == kIdle; }
    void SetIdle() { state = kPending; }

    bool IsFinish() const { return state == kFinish; }
    void SetFinish() { state = kFinish; }

    bool Contain(uint64_t offset, size_t n) {
      auto limit = file_req_offset + file_req_sz;
      return file_req_offset <= offset && (offset + n) <= limit;
    }

    // Check if the extent (offset, offset + n) has been much
    // exceeds this async buffer.
    bool Exceeds(uint64_t offset, size_t n) {
      auto limit = file_req_offset + file_req_sz;
      return offset >= limit;
    }

    void MaybeAllocateAsyncBuffer(size_t alloc_size) {
      const static uint64_t kPageSize = sysconf(_SC_PAGESIZE);
      // Use the existed buffer
      if (async_buf_ && async_buf_size_ >= alloc_size) {
        return;
      }
      alloc_size = round_up_align(alloc_size, kPageSize);
      // Release the previous allocated buffer
      if (async_buf_) {
        assert(IsIdle());
        free(async_buf_);
        async_buf_ = nullptr;
        async_buf_size_ = 0;
      }
      posix_memalign((void**)&async_buf_, kPageSize, alloc_size);
      if (async_buf_) {
        async_buf_size_ = alloc_size;
      }
    }

    void Read(uint64_t offset, size_t n, Slice* result, char* scratch) {
      assert(dev_req_offset <= file_dev_offset);
      auto delta =
          (offset - file_req_offset) + (file_dev_offset - dev_req_offset);
      std::memcpy(scratch, async_buf_ + delta, n);
      *result = Slice(scratch, n);
    }
  };

  FileAsyncReadRequest async_read_request_;

  void MaybeAllocateAsyncBuffer(size_t size);

 public:
  static const int SPARSE_HEADER_SIZE = 8;

  explicit ZoneFile(ZonedBlockDevice* zbd, uint64_t file_id_);

  virtual ~ZoneFile();

  void OpenWR(MetadataWriter* metadata_writer);
  IOStatus CloseWR();
  bool IsOpenForWR();
  IOStatus PersistMetadata();

  IOStatus Append(void* buffer, int data_size);
  IOStatus BufferedAppend(char* data, uint32_t size);
  IOStatus SparseAppend(char* data, uint32_t size);

  // Append the specified data slice atomically in a zone
  IOStatus BufferedAppendAtomic(char* data, uint32_t size);
  IOStatus AppendAtomic(void* buffer, int data_size);

  // Append the specified data slice of a value sst in a zone. Since a value
  // SST might be a flush SST or an output file of a GC task. This function
  // needs different dealing approach
  IOStatus ValueSSTAppend(char* data, uint32_t size);
  IOStatus GetZoneForFlushValueSSTWrite(Zone** zone);
  IOStatus GetZoneForGCValueSSTWrite(Zone** zone);

  // Get the corresponding partition according to current file type
  std::shared_ptr<ZonedBlockDevice::ZonePartition> GetPartition();

  IOStatus SetWriteLifeTimeHint(Env::WriteLifeTimeHint lifetime);
  void SetIOType(IOType io_type);
  std::string GetFilename();
  time_t GetFileModificationTime();
  void SetFileModificationTime(time_t mt);
  uint64_t GetFileSize();
  void SetFileSize(uint64_t sz);
  void ClearExtents();

  // ============================================================================
  // Project Modification
  // (xzw): level accessors
  // if the return level is -1, the file is not a KeySST
  // ============================================================================
  uint64_t GetLevel() const;
  void SetLevel(uint64_t level);

  // Extract the file number of this file via filename, return -1 if it's not
  // suffixed with ".sst".
  uint64_t ExtractFileNumber() {
    uint64_t fn = -1;

    auto fname = GetFilename();
    // Eliminate all preceding directories and only maintain the file number
    auto idx = fname.find_last_of('/');
    if (idx != fname.npos) {
      // Index started from idx + 1 because we need to ignore the charater '/'
      fname = fname.substr(idx + 1, fname.size() - idx - 1);
      if (fname.find(".sst") != fname.npos) {
        return std::stoull(fname.substr(0, fname.size() - strlen(".sst")));
      }
    }
    return -1;
  }

  PlacementFileType GetPlacementFileType() const;
  void SetPlacementFileType(PlacementFileType ftype);

  bool IsKeySST() const {
    return (io_type_ == IOType::kFlushFile ||
            io_type_ == IOType::kCompactionOutputFile) &&
           GetLevel() != -1;
  }

  bool IsValueSST() const {
    return (io_type_ == IOType::kFlushFile ||
            io_type_ == IOType::kCompactionOutputFile) &&
           GetLevel() == -1;
  }

  bool IsGCOutputValueSST() const {
    return (io_type_ == IOType::kCompactionOutputFile && GetLevel() == -1);
  }

  bool IsWAL() const { return io_type_ == IOType::kWAL; }

  // Record size of one write operation
  void RecordWrite(uint64_t size) const {
    if (IsKeySST()) {
      zbd_->GetXMetrics()->RecordWriteBytes(size, XZenFSMetricsType::kKeySST);
      if (level_ == 0) {
        zbd_->GetXMetrics()->RecordWriteBytes(size,
                                              XZenFSMetricsType::kKeySSTFlush);
      } else {
        zbd_->GetXMetrics()->RecordWriteBytes(
            size, XZenFSMetricsType::kKeySSTCompaction);
      }
    } else if (IsValueSST()) {
      zbd_->GetXMetrics()->RecordWriteBytes(size, XZenFSMetricsType::kValueSST);
      if (IsGCOutputValueSST()) {
        zbd_->GetXMetrics()->RecordWriteBytes(size,
                                              XZenFSMetricsType::kValueSSTGC);
      } else {
        zbd_->GetXMetrics()->RecordWriteBytes(
            size, XZenFSMetricsType::kValueSSTFlush);
      }
    } else if (IsWAL()) {
      zbd_->GetXMetrics()->RecordWriteBytes(size, XZenFSMetricsType::kWAL);
    }
  }

  Zone* GetBelongedZone() { return belonged_zone_; }

  // An approximation of the size of current file. For simplicity we use a hard
  // coding here. However, the reasonable approach is to read the exact size of
  // value sst from the dboption.
  // The DBOptions set a limit for the blob file size, however, do all blob
  // files obey this limitation?
  size_t ApproximateFileSize() { return (32 * 1024ULL * 1024ULL); }

  // ============================================================================
  //  Modification End
  // ============================================================================

  uint32_t GetBlockSize() { return zbd_->GetBlockSize(); }
  ZonedBlockDevice* GetZbd() { return zbd_; }
  std::vector<ZoneExtent*> GetExtents() { return extents_; }
  Env::WriteLifeTimeHint GetWriteLifeTimeHint() { return lifetime_; }

  IOStatus PositionedRead(uint64_t offset, size_t n, Slice* result,
                          char* scratch, bool direct);
  IOStatus PrefetchAsync(uint64_t offset, size_t n, bool direct);

  // A fast path for reading from async buffer if the requested data is
  // contained in the async buffer
  IOStatus MaybeReadFromAsyncBuffer(uint64_t offset, size_t n, Slice* result,
                                    char* scratch);

  ZoneExtent* GetExtent(uint64_t file_offset, uint64_t* dev_offset);
  void PushExtent();
  IOStatus AllocateNewZone();
  IOStatus AllocateNewZoneForWrite(size_t size);

  void EncodeTo(std::string* output, uint32_t extent_start);
  void EncodeUpdateTo(std::string* output) {
    EncodeTo(output, nr_synced_extents_);
  };
  void EncodeSnapshotTo(std::string* output) { EncodeTo(output, 0); };
  void EncodeJson(std::ostream& json_stream);
  void MetadataSynced() { nr_synced_extents_ = extents_.size(); };
  void MetadataUnsynced() { nr_synced_extents_ = 0; };

  IOStatus MigrateData(uint64_t offset, uint32_t length, Zone* target_zone);

  IOStatus ReadData(uint64_t offset, uint32_t length, char* data);
  IOStatus ReadData(uint64_t offset, uint32_t length, Slice* data);

  Status DecodeFrom(Slice* input);
  Status MergeUpdate(std::shared_ptr<ZoneFile> update, bool replace);

  uint64_t GetID() { return file_id_; }
  size_t GetUniqueId(char* id, size_t max_size);

  bool IsSparse() { return is_sparse_; };

  void SetSparse(bool is_sparse) { is_sparse_ = is_sparse; };
  uint64_t HasActiveExtent() { return extent_start_ != NO_EXTENT; };
  uint64_t GetExtentStart() { return extent_start_; };

  IOStatus Recover();

  void ReplaceExtentList(std::vector<ZoneExtent*> new_list);
  void ReplaceExtentListNoLock(std::vector<ZoneExtent*> new_list);
  void AddLinkName(const std::string& linkfile);
  IOStatus RemoveLinkName(const std::string& linkfile);
  IOStatus RenameLink(const std::string& src, const std::string& dest);
  uint32_t GetNrLinks() { return linkfiles_.size(); }
  const std::vector<std::string>& GetLinkFiles() const { return linkfiles_; }

 private:
  void ReleaseActiveZone();
  void SetActiveZone(Zone* zone);
  void SetBelongedZone(Zone* zone);
  IOStatus CloseActiveZone();

 public:
  std::shared_ptr<ZenFSMetrics> GetZBDMetrics() { return zbd_->GetMetrics(); };
  IOType GetIOType() const { return io_type_; };
  bool IsDeleted() const { return is_deleted_; };
  void SetDeleted() { is_deleted_ = true; };
  IOStatus RecoverSparseExtents(uint64_t start, uint64_t end, Zone* zone);
  ZenFSMetricsHistograms GetReportType() const;

 public:
  class ReadLock {
   public:
    ReadLock(ZoneFile* zfile) : zfile_(zfile) {
      zfile_->writer_mtx_.lock();
      zfile_->readers_++;
      zfile_->writer_mtx_.unlock();
    }
    ~ReadLock() { zfile_->readers_--; }

   private:
    ZoneFile* zfile_;
  };
  class WriteLock {
   public:
    WriteLock(ZoneFile* zfile) : zfile_(zfile) {
      zfile_->writer_mtx_.lock();
      while (zfile_->readers_ > 0) {
      }
    }
    ~WriteLock() { zfile_->writer_mtx_.unlock(); }

   private:
    ZoneFile* zfile_;
  };
};

class ZonedWritableFile : public FSWritableFile {
 public:
  explicit ZonedWritableFile(ZonedBlockDevice* zbd, bool buffered,
                             std::shared_ptr<ZoneFile> zoneFile,
                             MetadataWriter* metadata_writer = nullptr);
  virtual ~ZonedWritableFile();

  virtual IOStatus Append(const Slice& data, const IOOptions& options,
                          IODebugContext* dbg) override;
  virtual IOStatus Append(const Slice& data, const IOOptions& opts,
                          const DataVerificationInfo& /* verification_info */,
                          IODebugContext* dbg) override {
    return Append(data, opts, dbg);
  }
  virtual IOStatus PositionedAppend(const Slice& data, uint64_t offset,
                                    const IOOptions& options,
                                    IODebugContext* dbg) override;
  virtual IOStatus PositionedAppend(
      const Slice& data, uint64_t offset, const IOOptions& opts,
      const DataVerificationInfo& /* verification_info */,
      IODebugContext* dbg) override {
    return PositionedAppend(data, offset, opts, dbg);
  }
  virtual IOStatus Truncate(uint64_t size, const IOOptions& options,
                            IODebugContext* dbg) override;
  virtual IOStatus Close(const IOOptions& options,
                         IODebugContext* dbg) override;
  virtual IOStatus Flush(const IOOptions& options,
                         IODebugContext* dbg) override;
  virtual IOStatus Sync(const IOOptions& options, IODebugContext* dbg) override;
  virtual IOStatus RangeSync(uint64_t offset, uint64_t nbytes,
                             const IOOptions& options,
                             IODebugContext* dbg) override;
  virtual IOStatus Fsync(const IOOptions& options,
                         IODebugContext* dbg) override;

  bool use_direct_io() const override { return !buffered; }
  bool IsSyncThreadSafe() const override { return true; };
  size_t GetRequiredBufferAlignment() const override {
    return zoneFile_->GetBlockSize();
  }
  void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) override;
  virtual Env::WriteLifeTimeHint GetWriteLifeTimeHint() override {
    return zoneFile_->GetWriteLifeTimeHint();
  }
  void SetLevel(uint64_t level) { zoneFile_->SetLevel(level); }
  uint64_t GetLevel() const { return zoneFile_->GetLevel(); }

 private:
  IOStatus BufferedWrite(const Slice& data);
  IOStatus FlushBuffer();
  IOStatus DataSync();
  IOStatus CloseInternal();

  bool buffered;
  char* sparse_buffer;
  char* buffer;
  size_t buffer_sz;
  uint32_t block_sz;
  uint32_t buffer_pos;
  uint64_t wp;
  int write_temp;

  std::shared_ptr<ZoneFile> zoneFile_;
  MetadataWriter* metadata_writer_;

  std::mutex buffer_mtx_;
};

class ZonedSequentialFile : public FSSequentialFile {
 private:
  std::shared_ptr<ZoneFile> zoneFile_;
  uint64_t rp;
  bool direct_;

 public:
  explicit ZonedSequentialFile(std::shared_ptr<ZoneFile> zoneFile,
                               const FileOptions& file_opts)
      : zoneFile_(zoneFile),
        rp(0),
        direct_(file_opts.use_direct_reads && !zoneFile->IsSparse()) {}

  IOStatus Read(size_t n, const IOOptions& options, Slice* result,
                char* scratch, IODebugContext* dbg) override;
  IOStatus PositionedRead(uint64_t offset, size_t n, const IOOptions& options,
                          Slice* result, char* scratch,
                          IODebugContext* dbg) override;
  IOStatus Skip(uint64_t n) override;

  bool use_direct_io() const override { return direct_; };

  size_t GetRequiredBufferAlignment() const override {
    return zoneFile_->GetBlockSize();
  }

  IOStatus InvalidateCache(size_t /*offset*/, size_t /*length*/) override {
    return IOStatus::OK();
  }
};

class ZonedRandomAccessFile : public FSRandomAccessFile {
 private:
  std::shared_ptr<ZoneFile> zoneFile_;
  bool direct_;

 public:
  explicit ZonedRandomAccessFile(std::shared_ptr<ZoneFile> zoneFile,
                                 const FileOptions& file_opts)
      : zoneFile_(zoneFile),
        direct_(file_opts.use_direct_reads && !zoneFile->IsSparse()) {}

  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch,
                IODebugContext* dbg) const override;

  IOStatus Prefetch(uint64_t offset, size_t n, const IOOptions& options,
                    IODebugContext* /*dbg*/) override {
    return IOStatus::OK();
  }

  IOStatus PrefetchAsync(uint64_t offset, size_t n, const IOOptions& options,
                         IODebugContext*) override {
    if (zoneFile_) {
      return zoneFile_->PrefetchAsync(offset, n, true);
    }
    return IOStatus::OK();
  }

  bool use_direct_io() const override { return direct_; }

  size_t GetRequiredBufferAlignment() const override {
    return zoneFile_->GetBlockSize();
  }

  IOStatus InvalidateCache(size_t /*offset*/, size_t /*length*/) override {
    return IOStatus::OK();
  }

  size_t GetUniqueId(char* id, size_t max_size) const override;
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
