// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <chrono>
#include <cstdlib>
#include <cstring>

#include "fs/fs_zenfs.h"
#include "fs/log.h"
#include "fs/metrics.h"
#include "fs/util.h"
#include "fs/zbd_zenfs.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"
#include "rocksdb/metrics_reporter.h"
#if !defined(ROCKSDB_LITE) && !defined(OS_WIN)

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <libzbd/zbd.h>
#include <linux/blkzoned.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <cstring>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "io_zenfs.h"
#include "rocksdb/env.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {

ZoneExtent::ZoneExtent(uint64_t start, uint64_t length, Zone* zone)
    : start_(start), length_(length), zone_(zone) {}

Status ZoneExtent::DecodeFrom(Slice* input) {
  if (input->size() != (sizeof(start_) + sizeof(length_)))
    return Status::Corruption("ZoneExtent", "Error: length missmatch");

  GetFixed64(input, &start_);
  GetFixed64(input, &length_);
  return Status::OK();
}

void ZoneExtent::EncodeTo(std::string* output) {
  PutFixed64(output, start_);
  PutFixed64(output, length_);
}

void ZoneExtent::EncodeJson(std::ostream& json_stream) {
  json_stream << "{";
  json_stream << "\"start\":" << start_ << ",";
  json_stream << "\"length\":" << length_;
  json_stream << "}";
}

enum ZoneFileTag : uint32_t {
  kFileID = 1,

  // (zxw): for placement scheme
  kFileLevel = 2,
  kFileType = 3,

  kFileNameDeprecated = 4,
  kFileSize = 5,
  kWriteLifeTimeHint = 6,
  kExtent = 7,
  kModificationTime = 8,
  kActiveExtentStart = 9,
  kIsSparse = 10,
  kLinkedFilename = 11,

};

void ZoneFile::EncodeTo(std::string* output, uint32_t extent_start) {
  PutFixed32(output, kFileID);
  PutFixed64(output, file_id_);

  PutFixed32(output, kFileLevel);
  PutFixed64(output, level_);
  PutFixed32(output, kFileType);
  PutFixed64(output, uint64_t(io_type_));

  // ZnsLog(Color::kBlue, ">>>>> xzw >>>>> encoding level %lu of file %lu\n",
  //        level_, file_id_);

  PutFixed32(output, kFileSize);
  PutFixed64(output, file_size_);

  PutFixed32(output, kWriteLifeTimeHint);
  PutFixed32(output, (uint32_t)lifetime_);

  for (uint32_t i = extent_start; i < extents_.size(); i++) {
    std::string extent_str;

    PutFixed32(output, kExtent);
    extents_[i]->EncodeTo(&extent_str);
    PutLengthPrefixedSlice(output, Slice(extent_str));
  }

  PutFixed32(output, kModificationTime);
  PutFixed64(output, (uint64_t)m_time_);

  /* We store the current extent start - if there is a crash
   * we know that this file wrote the data starting from
   * active extent start up to the zone write pointer.
   * We don't need to store the active zone as we can look it up
   * from extent_start_ */
  PutFixed32(output, kActiveExtentStart);
  PutFixed64(output, extent_start_);

  if (is_sparse_) {
    PutFixed32(output, kIsSparse);
  }

  for (uint32_t i = 0; i < linkfiles_.size(); i++) {
    PutFixed32(output, kLinkedFilename);
    PutLengthPrefixedSlice(output, Slice(linkfiles_[i]));
  }
}

void ZoneFile::EncodeJson(std::ostream& json_stream) {
  json_stream << "{";
  json_stream << "\"id\":" << file_id_ << ",";
  json_stream << "\"size\":" << file_size_ << ",";
  json_stream << "\"hint\":" << lifetime_ << ",";
  json_stream << "\"extents\":[";

  for (const auto& name : GetLinkFiles())
    json_stream << "\"filename\":\"" << name << "\",";

  bool first_element = true;
  for (ZoneExtent* extent : extents_) {
    if (first_element) {
      first_element = false;
    } else {
      json_stream << ",";
    }
    extent->EncodeJson(json_stream);
  }
  json_stream << "]}";
}

Status ZoneFile::DecodeFrom(Slice* input) {
  uint32_t tag = 0;

  GetFixed32(input, &tag);
  if (tag != kFileID || !GetFixed64(input, &file_id_))
    return Status::Corruption("ZoneFile", "File ID missing");

  while (true) {
    Slice slice;
    ZoneExtent* extent;
    Status s;

    if (!GetFixed32(input, &tag)) break;

    switch (tag) {
      case kFileLevel:
        if (!GetFixed64(input, &level_))
          return Status::Corruption("ZoneFile", "Missing file level");
        // ZnsLog(Color::kBlue, ">>>>> xzw >>>>> decoding level %lu of file
        // %lu\n",
        //        level_, file_id_);
        break;
      case kFileType: {
        uint64_t tmp = 0;
        if (!GetFixed64(input, &tmp))
          return Status::Corruption("ZoneFile", "Missing file type");
        io_type_ = static_cast<IOType>(tmp);
        // ZnsLog(Color::kBlue,
        //        ">>>>> xzw >>>>> decoding level %lu and type %lu of file
        //        %lu\n", level_, uint64_t(io_type_), file_id_);
        break;
      }

      case kFileSize:
        if (!GetFixed64(input, &file_size_))
          return Status::Corruption("ZoneFile", "Missing file size");
        break;
      case kWriteLifeTimeHint:
        uint32_t lt;
        if (!GetFixed32(input, &lt))
          return Status::Corruption("ZoneFile", "Missing life time hint");
        lifetime_ = (Env::WriteLifeTimeHint)lt;
        break;
      case kExtent:
        extent = new ZoneExtent(0, 0, nullptr);
        GetLengthPrefixedSlice(input, &slice);
        s = extent->DecodeFrom(&slice);
        if (!s.ok()) {
          delete extent;
          return s;
        }
        extent->zone_ = zbd_->GetIOZone(extent->start_);
        if (!extent->zone_)
          return Status::Corruption("ZoneFile", "Invalid zone extent");
        extent->zone_->used_capacity_ += extent->length_;
        extents_.push_back(extent);
        break;
      case kModificationTime:
        uint64_t ct;
        if (!GetFixed64(input, &ct))
          return Status::Corruption("ZoneFile", "Missing creation time");
        m_time_ = (time_t)ct;
        break;
      case kActiveExtentStart:
        uint64_t es;
        if (!GetFixed64(input, &es))
          return Status::Corruption("ZoneFile", "Active extent start");
        extent_start_ = es;
        break;
      case kIsSparse:
        is_sparse_ = true;
        break;
      case kLinkedFilename:
        if (!GetLengthPrefixedSlice(input, &slice))
          return Status::Corruption("ZoneFile", "LinkFilename missing");

        if (slice.ToString().length() == 0)
          return Status::Corruption("ZoneFile", "Zero length Linkfilename");

        linkfiles_.push_back(slice.ToString());
        break;
      default:
        return Status::Corruption("ZoneFile", "Unexpected tag");
    }
  }

  MetadataSynced();
  return Status::OK();
}

Status ZoneFile::MergeUpdate(std::shared_ptr<ZoneFile> update, bool replace) {
  if (file_id_ != update->GetID())
    return Status::Corruption("ZoneFile update", "ID missmatch");

  SetFileSize(update->GetFileSize());
  SetWriteLifeTimeHint(update->GetWriteLifeTimeHint());
  SetFileModificationTime(update->GetFileModificationTime());

  // (xzw): Level info should be recovered for our placement scheme
  SetLevel(update->GetLevel());

  if (replace) {
    ClearExtents();
  }

  std::vector<ZoneExtent*> update_extents = update->GetExtents();
  for (long unsigned int i = 0; i < update_extents.size(); i++) {
    ZoneExtent* extent = update_extents[i];
    Zone* zone = extent->zone_;
    zone->used_capacity_ += extent->length_;
    extents_.push_back(new ZoneExtent(extent->start_, extent->length_, zone));
  }
  extent_start_ = update->GetExtentStart();
  is_sparse_ = update->IsSparse();
  MetadataSynced();

  linkfiles_.clear();
  for (const auto& name : update->GetLinkFiles()) linkfiles_.push_back(name);

  return Status::OK();
}

ZoneFile::ZoneFile(ZonedBlockDevice* zbd, uint64_t file_id)
    : zbd_(zbd),
      active_zone_(nullptr),
      extent_start_(NO_EXTENT),
      extent_filepos_(0),
      lifetime_(Env::WLTH_NOT_SET),
      io_type_(IOType::kUnknown),
      file_size_(0),
      file_id_(file_id),
      level_(uint64_t(-1)),
      place_ftype_(PlacementFileType::NoType()),
      belonged_zone_(nullptr),
      nr_synced_extents_(0),
      m_time_(0) {
  async_read_request_.Init();
}

std::string ZoneFile::GetFilename() { return linkfiles_[0]; }
time_t ZoneFile::GetFileModificationTime() { return m_time_; }

uint64_t ZoneFile::GetFileSize() { return file_size_; }
void ZoneFile::SetFileSize(uint64_t sz) { file_size_ = sz; }
void ZoneFile::SetFileModificationTime(time_t mt) { m_time_ = mt; }
void ZoneFile::SetIOType(IOType io_type) { io_type_ = io_type; }

// (xzw): level accessors
uint64_t ZoneFile::GetLevel() const { return level_; }
void ZoneFile::SetLevel(uint64_t level) { level_ = level; }

// (xzw): Placement hint
PlacementFileType ZoneFile::GetPlacementFileType() const {
  return place_ftype_;
}
void ZoneFile::SetPlacementFileType(PlacementFileType ftype) {
  place_ftype_ = ftype;
}

ZoneFile::~ZoneFile() {
  ClearExtents();
  IOStatus s = CloseWR();
  if (!s.ok()) {
    zbd_->SetZoneDeferredStatus(s);
  }
  // Note that the file number are added to zone-file map only when it is a
  // value sst
  if (belonged_zone_ && IsValueSST()) {
    auto fn = ExtractFileNumber();
    if (fn != -1) {
      zbd_->RemoveFileFromZone(fn, belonged_zone_->ZoneId());
    }
  }
}

void ZoneFile::ClearExtents() {
  for (auto e = std::begin(extents_); e != std::end(extents_); ++e) {
    Zone* zone = (*e)->zone_;

    assert(zone && zone->used_capacity_ >= (*e)->length_);
    zone->used_capacity_ -= (*e)->length_;
    delete *e;
  }
  extents_.clear();
}

IOStatus ZoneFile::CloseWR() {
  // ZnsLog(kGreen, "[kqh] File(%s) of id %lu with size %lu CloseWR",
  //        linkfiles_.front().c_str(), file_id_, file_size_);
  IOStatus s;
  if (open_for_wr_) {
    /* Mark up the file as being closed */
    extent_start_ = NO_EXTENT;
    s = PersistMetadata();
    if (!s.ok()) return s;
    open_for_wr_ = false;
    s = CloseActiveZone();
  }
  return s;
}

IOStatus ZoneFile::CloseActiveZone() {
  IOStatus s = IOStatus::OK();
  if (active_zone_) {
    bool full = active_zone_->IsFull();
    s = active_zone_->Close();
    ReleaseActiveZone();
    if (!s.ok()) {
      return s;
    }

    if (IsValueSST()) {
      // (kqh): For flush value sst, there is no need to return active or open
      // token on file close. Each partition (Hot/Warm/Partition) occupies an
      // active and open zone token for the whole time
      return s;
    }

    zbd_->PutOpenIOZoneToken();
    if (IsWAL()) {
      zbd_->wal_zone_open_count_--;
      ZnsLog(kMagenta, "WALOpenZoneCount(%d) WALActiveZoneCount(%d)",
             zbd_->wal_zone_open_count_.load(),
             zbd_->wal_zone_active_count_.load());
    }
    if (full) {
      zbd_->PutActiveIOZoneToken();
      if (IsWAL()) {
        zbd_->wal_zone_active_count_--;
        ZnsLog(kMagenta, "WALOpenZoneCount(%d) WALActiveZoneCount(%d)",
               zbd_->wal_zone_open_count_.load(),
               zbd_->wal_zone_active_count_.load());
      }
    }
  }
  // Key SST should not hold a valid active zone when closed. KeySST only
  // temporarily holds an active zone during an Append call and release this
  // active zone immediately after this append is finished or error happens
  if (IsKeySST()) {
    assert(active_zone_ == nullptr);
  }
  return s;
}

void ZoneFile::OpenWR(MetadataWriter* metadata_writer) {
  open_for_wr_ = true;
  metadata_writer_ = metadata_writer;
}

bool ZoneFile::IsOpenForWR() { return open_for_wr_; }

IOStatus ZoneFile::PersistMetadata() {
  assert(metadata_writer_ != NULL);
  return metadata_writer_->Persist(this);
}

ZoneExtent* ZoneFile::GetExtent(uint64_t file_offset, uint64_t* dev_offset) {
  for (unsigned int i = 0; i < extents_.size(); i++) {
    if (file_offset < extents_[i]->length_) {
      *dev_offset = extents_[i]->start_ + file_offset;
      return extents_[i];
    } else {
      file_offset -= extents_[i]->length_;
    }
  }
  return NULL;
}

IOStatus ZoneFile::PositionedRead(uint64_t offset, size_t n, Slice* result,
                                  char* scratch, bool direct) {
  ZenFSMetricsLatencyGuard guard(zbd_->GetMetrics(), ZENFS_READ_LATENCY,
                                 Env::Default());
  zbd_->GetMetrics()->ReportQPS(ZENFS_READ_QPS, 1);
  zbd_->GetMetrics()->ReportThroughput(ZENFS_ZONE_READ_THROUGHPUT, n);

  if (IsValueSST()) {
    // ZnsLog(kMagenta, "ZoneFile(%s) PositionedRead(%lu, %lu)",
    //        GetFilename().c_str(), offset, n);
  }

  ReadLock lck(this);

  // Fast path for reading from AsyncIO buffer
  // auto stat = MaybeReadFromAsyncBuffer(offset, n, result, scratch);
  // if (stat.ok()) {
  //   return stat;
  // }

  // // The requested data has been much exceeding the async buffer, the data
  // // contained in this async buffer is no more useful. Reset it for next
  // // async read request
  // if (async_read_request_.Exceeds(offset, n)) {
  //   auto start = Env::Default()->NowMicros();
  //   // Actually we don't care the return value of the reset object
  //   auto s = async_read_request_.Reset();
  //   auto dura = Env::Default()->NowMicros() - start;
  //   // ZnsLog(kRed, "Reset Pass Time: %lu us", dura);
  // }

  int f = zbd_->GetReadFD();
  int f_direct = zbd_->GetReadDirectFD();
  char* ptr;
  uint64_t r_off;
  size_t r_sz;
  ssize_t r = 0;
  size_t read = 0;
  ZoneExtent* extent;
  uint64_t extent_end;
  IOStatus s;

  if (offset >= file_size_) {
    *result = Slice(scratch, 0);
    return IOStatus::OK();
  }

  auto start = Env::Default()->NowMicros();
  r_off = 0;
  extent = GetExtent(offset, &r_off);
  if (!extent) {
    /* read start beyond end of (synced) file data*/
    *result = Slice(scratch, 0);
    return s;
  }
  extent_end = extent->start_ + extent->length_;

  /* Limit read size to end of file */
  if ((offset + n) > file_size_)
    r_sz = file_size_ - offset;
  else
    r_sz = n;

  ptr = scratch;

  while (read != r_sz) {
    size_t pread_sz = r_sz - read;

    if ((pread_sz + r_off) > extent_end) pread_sz = extent_end - r_off;

    /* We may get some unaligned direct reads due to non-aligned extent lengths,
     * so increase read request size to be aligned to next blocksize boundary.
     */
    bool aligned = (pread_sz % zbd_->GetBlockSize() == 0);

    size_t bytes_to_align = 0;
    if (direct && !aligned) {
      bytes_to_align = zbd_->GetBlockSize() - (pread_sz % zbd_->GetBlockSize());
      pread_sz += bytes_to_align;
      aligned = true;
    }

    if (direct && aligned) {
      r = pread(f_direct, ptr, pread_sz, r_off);
    } else {
      r = pread(f, ptr, pread_sz, r_off);
    }

    if (r <= 0) {
      if (r == -1 && errno == EINTR) {
        continue;
      }
      break;
    }

    /* Verify and update the the bytes read count (if read size was incremented,
     * for alignment purposes).
     */
    if ((size_t)r <= pread_sz - bytes_to_align)
      pread_sz = (size_t)r;
    else
      pread_sz -= bytes_to_align;

    ptr += pread_sz;
    read += pread_sz;
    r_off += pread_sz;

    if (read != r_sz && r_off == extent_end) {
      extent = GetExtent(offset + read, &r_off);
      if (!extent) {
        /* read beyond end of (synced) file data */
        break;
      }
      r_off = extent->start_;
      extent_end = extent->start_ + extent->length_;
    }
  }

  if (r < 0) {
    s = IOStatus::IOError("pread error\n");
    read = 0;
  }

  auto end = Env::Default()->NowMicros();
  if (IsValueSST()) {
    // ZnsLog(kMagenta, "ZoneFile(%s) Positioned Read (%lu, %lu): %lu us",
    //        GetFilename().c_str(), offset, n, end-start);
  }

  *result = Slice((char*)scratch, read);
  return s;
}

IOStatus ZoneFile::PrefetchAsync(uint64_t offset, size_t n, bool direct) {
  static const uint64_t kPageSize = sysconf(_SC_PAGESIZE);

  // For simplicity we only allow prefetch data within the file.
  if (offset + n >= GetFileSize()) {
    // ZnsLog(kMagenta,
    //        "%s Prefetch (%lu, %lu) rejected for out-of-range prefetch",
    //        GetFilename().c_str(), offset, n);
    return IOStatus::AsyncError("Requested prefetch extent exceeds the file");
  }

  // ZnsLog(kMagenta, "ZonedRandomAccessFile(%s) PrefetchAsync (%lu, %lu)",
  //        GetFilename().c_str(), offset, n);

  // There is already an async request pending or finished, it is has not
  // been explicitly reset by the user, reject current async fetch request
  if (!async_read_request_.IsIdle()) {
    // ZnsLog(kMagenta, "%s Prefetch Failed for current request is pending",
    //        GetFilename().c_str());
    return IOStatus::AsyncError("PrefetchAsync Request busy");
  }

  // We need to use Direct IO to access the data if we expect any benefit
  // from asynchronous IO. See:  https://github.com/axboe/fio/issues/512
  // for more information
  assert(direct == true);
  assert(async_read_request_.IsIdle());

  IOStatus s = IOStatus::OK();

  //
  // Translate the file offset to a device address: Note that we assume the
  // request data is within one extent. This is true for all blob files since
  // all contents of a blob file are written in the same zone.
  //
  // The dev_offset needs to be aligned with page, which is mandatory for
  // direct IO
  //
  uint64_t dev_offset = -1;
  auto extent = GetExtent(offset, &dev_offset);
  assert(dev_offset != -1 && extent != nullptr);
  // The requested data can not be separated
  assert(offset + n <= (extent->start_ + extent->length_));

  auto aligned_dev_offset = round_down_align(dev_offset, kPageSize);
  // size to read for aligned offset
  auto aligned_n = dev_offset + n - aligned_dev_offset;
  auto read_sz = round_up_align(aligned_n, kPageSize);

  async_read_request_.MaybeAllocateAsyncBuffer(read_sz);

  // Set the request states accordingly
  async_read_request_.file_req_offset = offset;
  async_read_request_.file_req_sz = n;
  async_read_request_.file_dev_offset = dev_offset;
  async_read_request_.dev_req_offset = aligned_dev_offset;
  async_read_request_.dev_req_sz = read_sz;

  s = async_read_request_.io_req.Init();
  if (!s.ok()) {
    assert(false);
    return s;
  }

  async_read_request_.io_req.PrepareRead(zbd_->GetReadDirectFD(), read_sz,
                                         aligned_dev_offset,
                                         async_read_request_.async_buf_);
  s = async_read_request_.io_req.Submit();
  if (!s.ok()) {
    assert(false);
    return s;
  }

  async_read_request_.SetPending();

  // ZnsLog(kMagenta,
  //        "ZonedRandomAccessFile(%s) PrefetchAsync (%lu, %lu) Successfully:"
  //        "file_req_offset(%lu), file_req_sz(%lu), file_dev_offset(%lu), "
  //        "dev_req_offset(%lu), dev_req_sz(%lu)",
  //        GetFilename().c_str(), offset, n,
  //        async_read_request_.file_req_offset,
  //        async_read_request_.file_req_sz,
  //        async_read_request_.file_dev_offset,
  //        async_read_request_.dev_req_offset, async_read_request_.dev_req_sz);
  return IOStatus::OK();
}

IOStatus ZoneFile::MaybeReadFromAsyncBuffer(uint64_t offset, size_t n,
                                            Slice* result, char* scratch) {
  auto start = Env::Default()->NowMicros();
  if (!async_read_request_.Contain(offset, n)) {
    // ZnsLog(kMagenta, "ZoneFile(%s) MaybeReadFromAsyncBuffer not match",
    //        GetFilename().c_str());
    return IOStatus::AsyncError(
        "Read request is not contained in the async buffer");
  }

  //
  // The requested data is contained in the async buffer, however, the async
  // request might have not been finished yet.
  //
  // Either this request is not finished or some error happens, returning an
  // error code will notify the callers to use sync read for their expected
  // data.
  //
  if (async_read_request_.IsPending()) {
    auto s = async_read_request_.io_req.CheckFinish();
    if (!s.ok()) {
      return s;
    }
    async_read_request_.SetFinish();
  }

  assert(async_read_request_.IsFinish());

  // Previous async IO finished: copy the data to the user requested
  // buffer. However, this still triggers a memory copy, I'm suspecting
  // if the upper layer can directly use the async buffer here
  //
  async_read_request_.Read(offset, n, result, scratch);
  auto end = Env::Default()->NowMicros();
  // ZnsLog(kMagenta,
  //        "ZoneFile(%s) Read From AsyncBuffer successfully (%lu,%lu): %lu us",
  //        GetFilename().c_str(), offset, n, end - start);

  return IOStatus::OK();
}

void ZoneFile::PushExtent() {
  uint64_t length;

  assert(file_size_ >= extent_filepos_);

  if (!active_zone_) return;

  length = file_size_ - extent_filepos_;
  if (length == 0) return;

  assert(length <= (active_zone_->wp_ - extent_start_));
  extents_.push_back(new ZoneExtent(extent_start_, length, active_zone_));

  active_zone_->used_capacity_ += length;
  extent_start_ = active_zone_->wp_;
  extent_filepos_ = file_size_;
}

IOStatus ZoneFile::AllocateNewZone() {
  Zone* zone;
  IOStatus s = zbd_->AllocateIOZone(lifetime_, io_type_, &zone);

  if (this->active_zone_) {
    assert(zone->ZoneId() == this->active_zone_->ZoneId());
  }
  if (!s.ok()) return s;
  if (!zone) {
    ZnsLog(kRed, "[kqh] File(%s) Allocate Zone Failed",
           linkfiles_.front().c_str());
    return IOStatus::NoSpace("Zone allocation failure");
  }
  SetActiveZone(zone);
  SetBelongedZone(zone);
  extent_start_ = active_zone_->wp_;
  extent_filepos_ = file_size_;

  if (!linkfiles_.empty()) {
    ZnsLog(kGreen, "[kqh] File(%s) Allocate Zone(%s)",
           linkfiles_.front().c_str(), zone->ToString().c_str());
  }

  /* Persist metadata so we can recover the active extent using
     the zone write pointer in case there is a crash before syncing */
  return PersistMetadata();
}

IOStatus ZoneFile::AllocateNewZoneForWrite(size_t size) {
  Zone* zone;
  IOStatus s;

  // [kqh] Allocate Zones based on file type and provided hint
  switch (io_type_) {
    case IOType::kWAL: {
      WALZoneAllocationHint hint(size, this);
      s = zbd_->AllocateWALZone(&zone, &hint);
      break;
    }
    case IOType::kCompactionOutputFile:
    case IOType::kFlushFile: {
      /* (xzw:TODO)
       * For performance monitoring does not distinguish KeySST and ValueSST
       * but both of them are labeled either kCompactionOutputFile or
       * kFlushFile. We can distinguish them here by checking whether level
       * if -1 (ValueSST).
       */
      if (level_ == -1) {
        ValueSSTZoneAllocationHint hint;
        s = zbd_->AllocateValueSSTZone(&zone, &hint);
      } else {
        KeySSTZoneAllocationHint hint(size, level_, GetFilename());
        s = zbd_->AllocateKeySSTZone(&zone, &hint);
      }
      break;
    }
    default: {
      s = zbd_->AllocateIOZone(lifetime_, io_type_, &zone);
      break;
    }
  }

  if (!s.ok()) return s;
  if (!zone) {
    return IOStatus::NoSpace("Zone allocation failure\n");
  }

  SetActiveZone(zone);
  SetBelongedZone(zone);
  extent_start_ = active_zone_->wp_;
  extent_filepos_ = file_size_;

  return PersistMetadata();
}

IOStatus ZoneFile::BufferedAppendAtomic(char* buffer, uint32_t data_size) {
  WriteLock lck(this);

  ZnsLog(kDisableLog, "[File %s] call BufferedAppendAtomic (size=%llu)",
         GetFilename().c_str(), data_size);

  uint32_t wr_size = data_size;
  uint32_t block_sz = GetBlockSize();
  IOStatus s;

  // Padding the buffered data to make it block-aligned
  uint32_t align = wr_size % block_sz;
  uint32_t pad_sz = 0;
  uint64_t extent_length = wr_size;

  if (align) pad_sz = block_sz - align;

  /* the buffer size s aligned on block size, so this is ok*/
  if (pad_sz) memset(buffer + wr_size, 0x0, pad_sz);

  s = AllocateNewZoneForWrite(wr_size + pad_sz);
  if (!s.ok() || !active_zone_) {
    return s;
  }

  s = active_zone_->Append(buffer, wr_size + pad_sz);

  // Release the active zone immediately if this write fails in case
  // of deadlock occures upon this zone. For example, this function acquires
  // the lock of the active_zone_ but fails to append data, then the zone must
  // be released before this function returns
  if (!s.ok()) {
    ReleaseActiveZone();
    return s;
  }

  RecordWrite(wr_size + pad_sz);

  extents_.push_back(
      new ZoneExtent(extent_start_, extent_length, active_zone_));
  active_zone_->used_capacity_ += extent_length;
  file_size_ += extent_length;

  ZnsLog(
      kDisableLog,
      "[kqh] File(%s) BufferedAppendAtomic (size=%u filesize=%lf) To Zone(%d)",
      GetFilename().c_str(), data_size, ToMiB(file_size_),
      active_zone_->ZoneId());

  ReleaseActiveZone();
  return IOStatus::OK();
}

/* Byte-aligned writes without a sparse header */
IOStatus ZoneFile::BufferedAppend(char* buffer, uint32_t data_size) {
  if (IsKeySST()) {
    return BufferedAppendAtomic(buffer, data_size);
  }

  WriteLock lck(this);

  uint32_t left = data_size;
  uint32_t wr_size;
  uint32_t block_sz = GetBlockSize();
  IOStatus s;

  if (active_zone_ == nullptr) {
    s = AllocateNewZone();
    if (!s.ok()) return s;
  }

  while (left) {
    wr_size = left;
    if (wr_size > active_zone_->capacity_) wr_size = active_zone_->capacity_;

    /* Pad to the next block boundary if needed */
    uint32_t align = wr_size % block_sz;
    uint32_t pad_sz = 0;

    if (align) pad_sz = block_sz - align;

    /* the buffer size s aligned on block size, so this is ok*/
    if (pad_sz) memset(buffer + wr_size, 0x0, pad_sz);

    uint64_t extent_length = wr_size;

    ZnsLog(
        kDisableLog,
        "[kqh] File(%s) BufferedAppend %lf MiB to Zone(%d), FileSize %lf MiB",
        GetFilename().c_str(), ToMiB(wr_size + pad_sz), active_zone_->ZoneId(),
        ToMiB(file_size_));

    s = active_zone_->Append(buffer, wr_size + pad_sz);
    if (!s.ok()) return s;

    RecordWrite(wr_size + pad_sz);

    extents_.push_back(
        new ZoneExtent(extent_start_, extent_length, active_zone_));

    extent_start_ = active_zone_->wp_;
    active_zone_->used_capacity_ += extent_length;
    file_size_ += extent_length;
    left -= extent_length;

    if (active_zone_->capacity_ == 0) {
      s = CloseActiveZone();
      if (!s.ok()) {
        return s;
      }
      if (left) {
        memcpy((void*)(buffer), (void*)(buffer + wr_size), left);
      }
      s = AllocateNewZone();
      if (!s.ok()) return s;
    }
  }

  return IOStatus::OK();
}

/* Byte-aligned, sparse writes with inline metadata
   the caller reserves 8 bytes of data for a size header */
IOStatus ZoneFile::SparseAppend(char* sparse_buffer, uint32_t data_size) {
  WriteLock lck(this);

  uint32_t left = data_size;
  uint32_t wr_size;
  uint32_t block_sz = GetBlockSize();
  IOStatus s;

  if (active_zone_ == nullptr) {
    if (io_type_ == IOType::kWAL) {
      s = AllocateNewZoneForWrite(data_size);
    } else {
      s = AllocateNewZone();
    }
    if (!s.ok()) return s;
  }

  while (left) {
    wr_size = left + ZoneFile::SPARSE_HEADER_SIZE;
    if (wr_size > active_zone_->capacity_) wr_size = active_zone_->capacity_;

    /* Pad to the next block boundary if needed */
    uint32_t align = wr_size % block_sz;
    uint32_t pad_sz = 0;

    if (align) pad_sz = block_sz - align;

    /* the sparse buffer has block_sz extra bytes tail allocated for padding, so
     * this is safe */
    if (pad_sz) memset(sparse_buffer + wr_size, 0x0, pad_sz);

    uint64_t extent_length = wr_size - ZoneFile::SPARSE_HEADER_SIZE;
    EncodeFixed64(sparse_buffer, extent_length);

    ZnsLog(kDisableLog,
           "[kqh] File(%s) SparseAppend %lf MiB to Zone(%d), FileSize %lf MiB",
           GetFilename().c_str(), ToMiB(wr_size + pad_sz),
           active_zone_->ZoneId(), ToMiB(file_size_));

    s = active_zone_->Append(sparse_buffer, wr_size + pad_sz);
    if (!s.ok()) return s;

    RecordWrite(wr_size + pad_sz);

    extents_.push_back(
        new ZoneExtent(extent_start_ + ZoneFile::SPARSE_HEADER_SIZE,
                       extent_length, active_zone_));

    extent_start_ = active_zone_->wp_;
    active_zone_->used_capacity_ += extent_length;
    file_size_ += extent_length;
    left -= extent_length;

    if (active_zone_->capacity_ == 0) {
      s = CloseActiveZone();
      if (!s.ok()) {
        return s;
      }
      if (left) {
        memcpy((void*)(sparse_buffer + ZoneFile::SPARSE_HEADER_SIZE),
               (void*)(sparse_buffer + wr_size), left);
      }

      if (io_type_ == IOType::kWAL) {
        s = AllocateNewZoneForWrite(left);
      } else {
        s = AllocateNewZone();
      }
      if (!s.ok()) return s;
    }
  }

  return IOStatus::OK();
}

IOStatus ZoneFile::GetZoneForFlushValueSSTWrite(Zone** write_zone) {
  assert(!IsGCOutputValueSST());
  IOStatus s = IOStatus::OK();

  // Pick one zone to write according to its placement file type
  auto partition = GetPartition();
  bool need_new_zone = false;
  auto zone_id = partition->GetActivatedZone();
  auto prev_zone = zbd_->GetZone(zone_id);

  partition->MaybeResetPendingZones();

  // This partition has no activated zone yet
  if (prev_zone == nullptr) {
    need_new_zone = true;
  } else {
    prev_zone->LoopForAcquire();
    // This approximation might not be accurate. For BufferedAppend, there
    // might be some extra space for padding.
    if (prev_zone->GetCapacityLeft() < ApproximateFileSize()) {
      need_new_zone = true;
    }
  }

  // Use the previous activate zone for write
  if (!need_new_zone) {
    *write_zone = prev_zone;
    return s;
  }
  // Release the prev zone if it's not nullptr
  // We do not need to release active and open token since it can be
  // reused for next chosen activate zone
  if (prev_zone) {
    bool full = prev_zone->IsFull();
    zbd_->GetZoneGCStatsOf(zone_id)->wasted_size +=
        prev_zone->GetCapacityLeft();
    // Finish this zone when it has no enough space
    s = prev_zone->Finish();
    if (!s.ok()) {
      return s;
    }
    s = prev_zone->Close();
    prev_zone->CheckRelease();
    if (!s.ok()) {
      return s;
    }
  } else {
    // No activate zone yet, ask for active and open token
    while (!zbd_->GetActiveIOZoneTokenIfAvailable())
      ;
    zbd_->WaitForOpenIOZoneToken(true);
  }
  auto new_zone_id = zbd_->PopEmptyZone();
  if (new_zone_id == kInvalidZoneId) {
    return IOStatus::NoSpace("ValueSST acquire empty zone Failed");
  }

  auto new_zone = zbd_->GetZone(new_zone_id);
  partition->AddZone(new_zone_id);
  partition->SetActivateZone(new_zone_id);

  if (prev_zone) {
    ZnsLog(kCyan,
           "Partition(%s) Switch ActivateZone From Zone %lu(%lu MiB) to "
           "Zone %lu(%lu MiB)",
           place_ftype_.ToString().c_str(), zone_id,
           ToMiB(prev_zone->GetCapacityLeft()), new_zone_id,
           ToMiB(new_zone->GetCapacityLeft()));
  }

  new_zone->LoopForAcquire();
  *write_zone = new_zone;

  return s;
}

IOStatus ZoneFile::GetZoneForGCValueSSTWrite(Zone** write_zone) {
  // assert(IsGCOutputValueSST());
  auto partition = GetPartition();
  auto zone_id = partition->GetCurrGCWriteZone();
  auto gc_write_zone = zbd_->GetZone(zone_id);
  bool need_new_zone = false;
  IOStatus s = IOStatus::OK();

  partition->MaybeResetPendingZones();

  if (gc_write_zone) {
    gc_write_zone->LoopForAcquire();
    if (gc_write_zone->GetCapacityLeft() < ApproximateFileSize()) {
      need_new_zone = true;
      //
      // FinishCurrGCWriteZone does not release the Open/Active token
      // occupied by this gc_write_zone. The token will be released
      // when the db layer notifies the FS to check if the current gc
      // write zone can be finished at the end of one GC task.
      //
      // We do not need to return the active/open token here because
      // a new gc_write_zone is allocated for subsequent writing
      //
      s = partition->FinishCurrGCWriteZone();
      if (!s.ok()) {
        return s;
      }
      partition->AddZone(gc_write_zone->ZoneId());
      ZnsLog(kCyan, "Partition switch GC Zone");
    }
  } else {
    // If current partition has no GC write zone, we must allocate
    // one active and open token for the gc_write_zone of this partition.
    // These tokens remain valid until gc_write_zone is finished.
    need_new_zone = true;
    while (!zbd_->GetActiveIOZoneTokenIfAvailable())
      ;
    zbd_->WaitForOpenIOZoneToken(true);
  }

  if (!need_new_zone) {
    *write_zone = gc_write_zone;
    return s;
  }

  auto new_zone_id = zbd_->PopEmptyZone();
  if (new_zone_id == kInvalidZoneId) {
    return IOStatus::NoSpace("ValueSST acquire empty zone Failed");
  }

  auto new_zone = zbd_->GetZone(new_zone_id);
  new_zone->LoopForAcquire();

  partition->SetCurrGCWriteZone(new_zone_id);

  ZnsLog(kCyan, "Partition switch GC Zone to %lu", new_zone_id);
  *write_zone = new_zone;

  return s;
}

IOStatus ZoneFile::ValueSSTAppend(char* data, uint32_t data_size) {
  assert(IsValueSST());
  // ZnsLog(kCyan, "ValueSSTAppend::Start (IsGC: %lu)", IsGCOutputValueSST());
  // Defer d([=]() {
  //   ZnsLog(kCyan, "ValueSSTAppend::End (IsGC: %lu)",
  //          this->IsGCOutputValueSST());
  // });

  uint32_t left = data_size;
  uint32_t wr_size, offset = 0;
  IOStatus s = IOStatus::OK();

  if (!active_zone_) {
    //
    // Pick a zone for writing accordingly:
    //
    //   * For normal flush value sst, just pick current activated zone and
    //     switch the activated zone if necessary (i.e. no enough space to
    //     write).
    //
    //   * For GC output sst, just pick current gc_write_zone and switch
    //     the gc_write_zone if necessary.
    //
    // In most cases, there is only one thread writing flush SST or GC SST
    // for each partition.
    //
    Zone* write_zone = nullptr;
    if (!IsGCOutputValueSST()) {
      s = GetZoneForFlushValueSSTWrite(&write_zone);
      if (!s.ok()) {
        return s;
      }
    } else {
      // For partition zone and warm zone, their GC write must use an individual
      // zone in case of throughput intervention between flush and GC since
      // they have a large amount of data to write. However, we expect hot
      // partition to share flush and gc zone as they have fewer data to write.
      if (place_ftype_.IsHot()) {
        s = GetZoneForFlushValueSSTWrite(&write_zone);
      } else {
        s = GetZoneForGCValueSSTWrite(&write_zone);
      }
      if (!s.ok()) {
        return s;
      }
    }
    // Set all related meta data
    SetActiveZone(write_zone);
    SetBelongedZone(write_zone);
    extent_start_ = active_zone_->wp_;
    extent_filepos_ = file_size_;
    PersistMetadata();
  }
  assert(active_zone_ != nullptr);

  // This implementation causes this file occupy this zone exclusively. In our
  // design, we expect there is only one thread flushing the memtable. Multiple
  // threads flushing the memtable would require assigning individual zones
  // apiece.
  while (left) {
    // NOTE: We do not allow switching zone cause we require the value sst to
    // be completely written in a single zone
    if (active_zone_->capacity_ == 0) {
      assert(false);
    }

    wr_size = left;
    if (wr_size > active_zone_->capacity_) {
      // write across zone is not allowed
      assert(false);
    }

    ZnsLog(
        kDisableLog,
        "[kqh] File(%s) ValueSSTAppend %lf MiB to Zone(%d), FileSize %lf MiB",
        GetFilename().c_str(), ToMiB(wr_size), active_zone_->ZoneId(),
        ToMiB(file_size_));

    s = active_zone_->Append((char*)data + offset, wr_size);
    if (!s.ok()) return s;

    RecordWrite(wr_size);

    file_size_ += wr_size;
    left -= wr_size;
    offset += wr_size;
  }
  return IOStatus::OK();
}

std::shared_ptr<ZonedBlockDevice::ZonePartition> ZoneFile::GetPartition() {
  if (place_ftype_.IsHot()) {
    return zbd_->hot_partition_;
  } else if (place_ftype_.IsWarm()) {
    return zbd_->warm_partition_;
  } else if (place_ftype_.IsPartition()) {
    return zbd_->hash_partitions_[place_ftype_.PartitionId()];
  } else {
    return nullptr;
  }
}

/* Assumes that data and size are block aligned */
IOStatus ZoneFile::Append(void* data, int data_size) {
  // Different write path according to the specific file type
  if (IsKeySST()) {
    return AppendAtomic(data, data_size);
  }
  if (IsValueSST()) {
    return ValueSSTAppend((char*)data, data_size);
  }

  WriteLock lck(this);

  uint32_t left = data_size;
  uint32_t wr_size, offset = 0;
  IOStatus s = IOStatus::OK();

  if (!active_zone_) {
    s = AllocateNewZone();
    if (!s.ok()) return s;
  }

  while (left) {
    if (active_zone_->capacity_ == 0) {
      PushExtent();

      s = CloseActiveZone();
      if (!s.ok()) {
        return s;
      }

      s = AllocateNewZone();
      if (!s.ok()) return s;
    }

    wr_size = left;
    if (wr_size > active_zone_->capacity_) wr_size = active_zone_->capacity_;

    ZnsLog(kDisableLog,
           "[kqh] File(%s) SimpleAppend %lf MiB to Zone(%d), FileSize %lf MiB",
           GetFilename().c_str(), ToMiB(wr_size), active_zone_->ZoneId(),
           ToMiB(file_size_));

    s = active_zone_->Append((char*)data + offset, wr_size);
    if (!s.ok()) return s;

    RecordWrite(wr_size);

    file_size_ += wr_size;
    left -= wr_size;
    offset += wr_size;
  }

  return IOStatus::OK();
}

IOStatus ZoneFile::AppendAtomic(void* buffer, int data_size) {
  WriteLock lck(this);

  ZnsLog(kRed, "[File %s] call AppendAtomic (size=%llu)", GetFilename().c_str(),
         data_size);

  uint32_t left = data_size;
  uint32_t wr_size = left, offset = 0;
  uint32_t extent_length = data_size;
  IOStatus s = IOStatus::OK();

  s = AllocateNewZoneForWrite(data_size);
  if (!s.ok() || !active_zone_) {
    return s;
  }

  s = active_zone_->Append((char*)buffer, wr_size);

  // Release the active zone immediately if this write fails in case
  // of deadlock occures upon this zone. For example, this function acquires
  // the lock of the active_zone_ but fails to append data, then the zone must
  // be released before this function returns
  if (!s.ok()) {
    ReleaseActiveZone();
    return s;
  }

  RecordWrite(wr_size);

  // For AppendAtomic, construct an extent for each invoke.
  extents_.push_back(
      new ZoneExtent(extent_start_, extent_length, active_zone_));
  active_zone_->used_capacity_ += extent_length;
  file_size_ += extent_length;

  ZnsLog(kDisableLog,
         "[kqh] File(%s) SingleAppendAtomic (size=%u filesize=%lf) To Zone(%d)",
         GetFilename().c_str(), data_size, ToMiB(file_size_),
         active_zone_->ZoneId());

  // Release the active zone immediately
  ReleaseActiveZone();

  return IOStatus::OK();
}

IOStatus ZoneFile::RecoverSparseExtents(uint64_t start, uint64_t end,
                                        Zone* zone) {
  /* Sparse writes, we need to recover each individual segment */
  IOStatus s;
  uint32_t block_sz = GetBlockSize();
  int f = zbd_->GetReadFD();
  uint64_t next_extent_start = start;
  char* buffer;
  int recovered_segments = 0;
  int ret;

  ret = posix_memalign((void**)&buffer, sysconf(_SC_PAGESIZE), block_sz);
  if (ret) {
    return IOStatus::IOError("Out of memory while recovering");
  }

  while (next_extent_start < end) {
    uint64_t extent_length;

    ret = pread(f, (void*)buffer, block_sz, next_extent_start);
    if (ret != (int)block_sz) {
      s = IOStatus::IOError("Unexpected read error while recovering");
      break;
    }

    extent_length = DecodeFixed64(buffer);
    if (extent_length == 0) {
      s = IOStatus::IOError("Unexexpeted extent length while recovering");
      break;
    }
    recovered_segments++;

    zone->used_capacity_ += extent_length;
    extents_.push_back(new ZoneExtent(next_extent_start + SPARSE_HEADER_SIZE,
                                      extent_length, zone));

    uint64_t extent_blocks = (extent_length + SPARSE_HEADER_SIZE) / block_sz;
    if ((extent_length + SPARSE_HEADER_SIZE) % block_sz) {
      extent_blocks++;
    }
    next_extent_start += extent_blocks * block_sz;
  }

  free(buffer);
  return s;
}

IOStatus ZoneFile::Recover() {
  /* If there is no active extent, the file was either closed gracefully
     or there were no writes prior to a crash. All good.*/
  if (!HasActiveExtent()) return IOStatus::OK();

  /* Figure out which zone we were writing to */
  Zone* zone = zbd_->GetIOZone(extent_start_);

  if (zone == nullptr) {
    return IOStatus::IOError(
        "Could not find zone for extent start while recovering");
  }

  if (zone->wp_ < extent_start_) {
    return IOStatus::IOError("Zone wp is smaller than active extent start");
  }

  /* How much data do we need to recover? */
  uint64_t to_recover = zone->wp_ - extent_start_;

  /* Do we actually have any data to recover? */
  if (to_recover == 0) {
    /* Mark up the file as having no missing extents */
    extent_start_ = NO_EXTENT;
    return IOStatus::OK();
  }

  /* Is the data sparse or was it writted direct? */
  if (is_sparse_) {
    IOStatus s = RecoverSparseExtents(extent_start_, zone->wp_, zone);
    if (!s.ok()) return s;
  } else {
    /* For non-sparse files, the data is contigous and we can recover directly
       any missing data using the WP */
    zone->used_capacity_ += to_recover;
    extents_.push_back(new ZoneExtent(extent_start_, to_recover, zone));
  }

  /* Mark up the file as having no missing extents */
  extent_start_ = NO_EXTENT;

  /* Recalculate file size */
  file_size_ = 0;
  for (uint32_t i = 0; i < extents_.size(); i++) {
    file_size_ += extents_[i]->length_;
  }

  return IOStatus::OK();
}

void ZoneFile::ReplaceExtentList(std::vector<ZoneExtent*> new_list) {
  assert(!IsOpenForWR() && new_list.size() > 0);
  assert(new_list.size() == extents_.size());

  WriteLock lck(this);
  extents_ = new_list;
}

void ZoneFile::ReplaceExtentListNoLock(std::vector<ZoneExtent*> new_list) {
  // assert(!IsOpenForWR() && new_list.size() > 0);
  assert(new_list.size() > 0);
  assert(new_list.size() == extents_.size());

  extents_ = new_list;
}

void ZoneFile::AddLinkName(const std::string& linkf) {
  linkfiles_.push_back(linkf);
}

IOStatus ZoneFile::RenameLink(const std::string& src, const std::string& dest) {
  auto itr = std::find(linkfiles_.begin(), linkfiles_.end(), src);
  if (itr != linkfiles_.end()) {
    linkfiles_.erase(itr);
    linkfiles_.push_back(dest);
  } else {
    return IOStatus::IOError("RenameLink: Failed to find the linked file");
  }
  return IOStatus::OK();
}

IOStatus ZoneFile::RemoveLinkName(const std::string& linkf) {
  assert(GetNrLinks());
  auto itr = std::find(linkfiles_.begin(), linkfiles_.end(), linkf);
  if (itr != linkfiles_.end()) {
    linkfiles_.erase(itr);
  } else {
    return IOStatus::IOError("RemoveLinkInfo: Failed to find the link file");
  }
  return IOStatus::OK();
}

IOStatus ZoneFile::SetWriteLifeTimeHint(Env::WriteLifeTimeHint lifetime) {
  lifetime_ = lifetime;
  return IOStatus::OK();
}

void ZoneFile::ReleaseActiveZone() {
  assert(active_zone_ != nullptr);
  bool ok = active_zone_->Release();
  assert(ok);
  (void)ok;
  active_zone_ = nullptr;
}

void ZoneFile::SetActiveZone(Zone* zone) {
  assert(active_zone_ == nullptr);
  assert(zone->IsBusy());
  active_zone_ = zone;
}

void ZoneFile::SetBelongedZone(Zone* zone) {
  assert(zone->IsBusy());
  belonged_zone_ = zone;
  if (IsValueSST()) {
    // Only add file number of a value sst to the zone-file map when
    // it is a value sst
    auto fn = ExtractFileNumber();
    assert(fn != -1);
    zbd_->AddFileToZone(fn, belonged_zone_->ZoneId());
  }
}

ZenFSMetricsHistograms ZoneFile::GetReportType() const {
  ZenFSMetricsHistograms type;
  switch (GetIOType()) {
    case IOType::kWAL:
      return ZENFS_ZONE_WAL_WRITE_THROUGHPUT;
    case IOType::kFlushFile:
      return ZENFS_ZONE_FLUSH_FILE_WRITE_THROUGHPUT;
    case IOType::kCompactionOutputFile:
      return ZENFS_ZONE_COMPACTION_OUTPUT_FILE_WRITE_THROUGHPUT;
    default:
      // Invalid report type
      return ZENFS_HISTOGRAM_ENUM_MIN;
  }
}

ZonedWritableFile::ZonedWritableFile(ZonedBlockDevice* zbd, bool _buffered,
                                     std::shared_ptr<ZoneFile> zoneFile,
                                     MetadataWriter* metadata_writer) {
  wp = zoneFile->GetFileSize();

  buffered = _buffered;
  block_sz = zbd->GetBlockSize();
  zoneFile_ = zoneFile;
  buffer_pos = 0;
  sparse_buffer = nullptr;
  buffer = nullptr;

  if (buffered) {
    if (zoneFile->IsSparse()) {
      size_t sparse_buffer_sz;

      sparse_buffer_sz =
          1024 * 1024 + block_sz; /* one extra block size for padding */
      int ret = posix_memalign((void**)&sparse_buffer, sysconf(_SC_PAGESIZE),
                               sparse_buffer_sz);

      if (ret) sparse_buffer = nullptr;

      assert(sparse_buffer != nullptr);

      buffer_sz = sparse_buffer_sz - ZoneFile::SPARSE_HEADER_SIZE - block_sz;
      buffer = sparse_buffer + ZoneFile::SPARSE_HEADER_SIZE;
    } else {
      // buffer_sz = 1024 * 1024;
      // (kqh) Modify buffer size here so that one SSTable can be written
      // atomically
      if (zoneFile->IsValueSST()) {
        buffer_sz = 64 * 1024 * 1024;
      } else if (zoneFile->IsKeySST()) {
        buffer_sz = 32 * 1024 * 1024;
      } else {
        buffer_sz = 1024 * 1024;
      }

      ZnsLog(kRed, "[File %s Create buffer of size %llu]",
             zoneFile->GetFilename().c_str(), buffer_sz);

      int ret =
          posix_memalign((void**)&buffer, sysconf(_SC_PAGESIZE), buffer_sz);

      if (ret) buffer = nullptr;
      assert(buffer != nullptr);
    }
  }

  zoneFile_->OpenWR(metadata_writer);
}

ZonedWritableFile::~ZonedWritableFile() {
  IOStatus s = CloseInternal();
  if (buffered) {
    if (sparse_buffer != nullptr) {
      free(sparse_buffer);
    } else {
      free(buffer);
    }
  }

  if (!s.ok()) {
    zoneFile_->GetZbd()->SetZoneDeferredStatus(s);
  }
}

MetadataWriter::~MetadataWriter() {}

IOStatus ZonedWritableFile::Truncate(uint64_t size,
                                     const IOOptions& /*options*/,
                                     IODebugContext* /*dbg*/) {
  zoneFile_->SetFileSize(size);
  return IOStatus::OK();
}

IOStatus ZonedWritableFile::DataSync() {
  if (buffered) {
    IOStatus s;
    buffer_mtx_.lock();
    ZnsLog(kDisableLog, "[ZonedWritableFile::DataSync()] %s",
           zoneFile_->GetFilename().c_str());
    /* Flushing the buffer will result in a new extent added to the list*/
    s = FlushBuffer();
    buffer_mtx_.unlock();
    if (!s.ok()) {
      return s;
    }

    /* We need to persist the new extent, if the file is not sparse,
     * as we can't use the active zone WP, which is block-aligned, to recover
     * the file size */
    if (!zoneFile_->IsSparse()) return zoneFile_->PersistMetadata();
  } else {
    /* For direct writes, there is no buffer to flush, we just need to push
       an extent for the latest written data */
    zoneFile_->PushExtent();
  }

  return IOStatus::OK();
}

IOStatus ZonedWritableFile::Fsync(const IOOptions& /*options*/,
                                  IODebugContext* /*dbg*/) {
  IOStatus s;
  ZenFSMetricsLatencyGuard guard(zoneFile_->GetZBDMetrics(),
                                 zoneFile_->GetIOType() == IOType::kWAL
                                     ? ZENFS_WAL_SYNC_LATENCY
                                     : ZENFS_NON_WAL_SYNC_LATENCY,
                                 Env::Default());
  zoneFile_->GetZBDMetrics()->ReportQPS(ZENFS_SYNC_QPS, 1);

  s = DataSync();
  if (!s.ok()) return s;

  /* As we've already synced the metadata in DataSync, no need to do it again */
  if (buffered && !zoneFile_->IsSparse()) return IOStatus::OK();

  return zoneFile_->PersistMetadata();
}

IOStatus ZonedWritableFile::Sync(const IOOptions& /*options*/,
                                 IODebugContext* /*dbg*/) {
  return DataSync();
}

IOStatus ZonedWritableFile::Flush(const IOOptions& /*options*/,
                                  IODebugContext* /*dbg*/) {
  return IOStatus::OK();
}

IOStatus ZonedWritableFile::RangeSync(uint64_t offset, uint64_t nbytes,
                                      const IOOptions& /*options*/,
                                      IODebugContext* /*dbg*/) {
  if (wp < offset + nbytes) return DataSync();

  return IOStatus::OK();
}

IOStatus ZonedWritableFile::Close(const IOOptions& /*options*/,
                                  IODebugContext* /*dbg*/) {
  return CloseInternal();
}

IOStatus ZonedWritableFile::CloseInternal() {
  if (!zoneFile_->IsOpenForWR()) {
    return IOStatus::OK();
  }

  IOStatus s = DataSync();
  if (!s.ok()) return s;

  return zoneFile_->CloseWR();
}

IOStatus ZonedWritableFile::FlushBuffer() {
  IOStatus s;

  if (buffer_pos == 0) return IOStatus::OK();

  if (zoneFile_->IsSparse()) {
    s = zoneFile_->SparseAppend(sparse_buffer, buffer_pos);
  } else {
    s = zoneFile_->BufferedAppend(buffer, buffer_pos);
  }

  if (!s.ok()) {
    return s;
  }

  wp += buffer_pos;
  buffer_pos = 0;

  return IOStatus::OK();
}

IOStatus ZonedWritableFile::BufferedWrite(const Slice& slice) {
  uint32_t data_left = slice.size();
  char* data = (char*)slice.data();
  IOStatus s;

  while (data_left) {
    uint32_t buffer_left = buffer_sz - buffer_pos;
    uint32_t to_buffer;

    if (!buffer_left) {
      s = FlushBuffer();
      if (!s.ok()) return s;
      buffer_left = buffer_sz;
    }

    to_buffer = std::min(buffer_left, data_left);

    memcpy(buffer + buffer_pos, data, to_buffer);
    buffer_pos += to_buffer;
    data_left -= to_buffer;
    data += to_buffer;
  }

  return IOStatus::OK();
}

IOStatus ZonedWritableFile::Append(const Slice& data,
                                   const IOOptions& /*options*/,
                                   IODebugContext* /*dbg*/) {
  IOStatus s;
  ZenFSMetricsLatencyGuard guard(zoneFile_->GetZBDMetrics(),
                                 zoneFile_->GetIOType() == IOType::kWAL
                                     ? ZENFS_WAL_WRITE_LATENCY
                                     : ZENFS_NON_WAL_WRITE_LATENCY,
                                 Env::Default());
  zoneFile_->GetZBDMetrics()->ReportQPS(ZENFS_WRITE_QPS, 1);
  zoneFile_->GetZBDMetrics()->ReportThroughput(ZENFS_WRITE_THROUGHPUT,
                                               data.size());

  // (kqh) Report write throughput
  auto type = zoneFile_->GetReportType();
  if (type != ZENFS_HISTOGRAM_ENUM_MIN) {
    zoneFile_->GetZBDMetrics()->ReportThroughput(type, data.size());
  }

  if (buffered) {
    buffer_mtx_.lock();
    s = BufferedWrite(data);
    buffer_mtx_.unlock();
  } else {
    s = zoneFile_->Append((void*)data.data(), data.size());
    if (s.ok()) wp += data.size();
  }

  return s;
}

IOStatus ZonedWritableFile::PositionedAppend(const Slice& data, uint64_t offset,
                                             const IOOptions& /*options*/,
                                             IODebugContext* /*dbg*/) {
  IOStatus s;
  ZenFSMetricsLatencyGuard guard(zoneFile_->GetZBDMetrics(),
                                 zoneFile_->GetIOType() == IOType::kWAL
                                     ? ZENFS_WAL_WRITE_LATENCY
                                     : ZENFS_NON_WAL_WRITE_LATENCY,
                                 Env::Default());
  zoneFile_->GetZBDMetrics()->ReportQPS(ZENFS_WRITE_QPS, 1);
  zoneFile_->GetZBDMetrics()->ReportThroughput(ZENFS_WRITE_THROUGHPUT,
                                               data.size());

  // (kqh): Report write throughput
  auto type = zoneFile_->GetReportType();
  if (type != ZENFS_HISTOGRAM_ENUM_MIN) {
    zoneFile_->GetZBDMetrics()->ReportThroughput(type, data.size());
  }

  if (offset != wp) {
    assert(false);
    return IOStatus::IOError("positioned append not at write pointer");
  }

  if (buffered) {
    buffer_mtx_.lock();
    s = BufferedWrite(data);
    buffer_mtx_.unlock();
  } else {
    s = zoneFile_->Append((void*)data.data(), data.size());
    if (s.ok()) wp += data.size();
  }

  return s;
}

void ZonedWritableFile::SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) {
  zoneFile_->SetWriteLifeTimeHint(hint);
}

IOStatus ZonedSequentialFile::Read(size_t n, const IOOptions& /*options*/,
                                   Slice* result, char* scratch,
                                   IODebugContext* /*dbg*/) {
  IOStatus s;

  s = zoneFile_->PositionedRead(rp, n, result, scratch, direct_);
  if (s.ok()) rp += result->size();

  return s;
}

IOStatus ZonedSequentialFile::Skip(uint64_t n) {
  if (rp + n >= zoneFile_->GetFileSize())
    return IOStatus::InvalidArgument("Skip beyond end of file");
  rp += n;
  return IOStatus::OK();
}

IOStatus ZonedSequentialFile::PositionedRead(uint64_t offset, size_t n,
                                             const IOOptions& /*options*/,
                                             Slice* result, char* scratch,
                                             IODebugContext* /*dbg*/) {
  return zoneFile_->PositionedRead(offset, n, result, scratch, direct_);
}

IOStatus ZonedRandomAccessFile::Read(uint64_t offset, size_t n,
                                     const IOOptions& /*options*/,
                                     Slice* result, char* scratch,
                                     IODebugContext* /*dbg*/) const {
  return zoneFile_->PositionedRead(offset, n, result, scratch, direct_);
}

size_t ZoneFile::GetUniqueId(char* id, size_t max_size) {
  /* Based on the posix fs implementation */
  if (max_size < kMaxVarint64Length * 3) {
    return 0;
  }

  struct stat buf;
  int fd = zbd_->GetReadFD();
  int result = fstat(fd, &buf);
  if (result == -1) {
    return 0;
  }

  char* rid = id;
  rid = EncodeVarint64(rid, buf.st_dev);
  rid = EncodeVarint64(rid, buf.st_ino);
  rid = EncodeVarint64(rid, file_id_);
  assert(rid >= id);
  return static_cast<size_t>(rid - id);

  return 0;
}

IOStatus ZoneFile::ReadData(uint64_t offset, uint32_t length, char* data) {
  int block_sz = zbd_->GetBlockSize();
  assert(offset % block_sz == 0);
  if (offset % block_sz != 0) {
    return IOStatus::IOError("ReadData offset is not aligned!\n");
  }

  int pad_sz = length % block_sz == 0 ? 0 : (block_sz - (length % block_sz));
  int r = zbd_->DirectRead(data, offset, length + pad_sz);
  if (r < 0) {
    return IOStatus::IOError(strerror(errno));
  }
  return IOStatus::OK();
}

IOStatus ZoneFile::ReadData(uint64_t offset, uint32_t length, Slice* data) {
  zbd_->GetMetrics()->ReportThroughput(ZENFS_ZONE_GC_READ_THROUGHPUT, length);
  int block_sz = zbd_->GetBlockSize();
  assert(offset % block_sz == 0);
  if (offset % block_sz != 0) {
    return IOStatus::IOError("ReadData offset is not aligned!\n");
  }
  int pad_sz = length % block_sz == 0 ? 0 : (block_sz - (length % block_sz));
  char* buf_ptr;
  posix_memalign((void**)&buf_ptr, block_sz, (length + pad_sz));
  int r = zbd_->DirectRead(buf_ptr, offset, length + pad_sz);
  if (r < 0) {
    return IOStatus::IOError(strerror(errno));
  }
  *data = Slice(buf_ptr, length + pad_sz);
  return IOStatus::OK();
}

// (kqh): TODO: May add throughput reporter here to record migration throughput
IOStatus ZoneFile::MigrateData(uint64_t offset, uint32_t length,
                               Zone* target_zone) {
  ZnsLog(kBlue, "[kqh] Migrate Data: offset(%zu) len(%u) to ZoneId(%d)", offset,
         length, target_zone->ZoneId());

  zbd_->GetMetrics()->ReportThroughput(ZENFS_ZONE_GC_READ_THROUGHPUT, length);
  uint32_t step = 128 << 10;
  uint32_t read_sz = step;
  int block_sz = zbd_->GetBlockSize();

  assert(offset % block_sz == 0);
  if (offset % block_sz != 0) {
    return IOStatus::IOError("MigrateData offset is not aligned!\n");
  }

  char* buf;
  int ret = posix_memalign((void**)&buf, block_sz, step);
  if (ret) {
    return IOStatus::IOError("failed allocating alignment write buffer\n");
  }

  int pad_sz = 0;
  while (length > 0) {
    read_sz = length > read_sz ? read_sz : length;
    pad_sz = read_sz % block_sz == 0 ? 0 : (block_sz - (read_sz % block_sz));

    int r = zbd_->DirectRead(buf, offset, read_sz + pad_sz);
    if (r < 0) {
      free(buf);
      return IOStatus::IOError(strerror(errno));
    }
    target_zone->Append(buf, r, true);
    zbd_->GetXMetrics()->RecordWriteBytes(r, kGCMigrate);
    length -= read_sz;
    offset += r;
  }

  free(buf);

  return IOStatus::OK();
}

void ZoneFile::MaybeAllocateAsyncBuffer(size_t alloc_size) {
  // const static uint64_t kPageSize = sysconf(_SC_PAGESIZE);
  // // Use the existed buffer
  // if (async_buf_ && async_buf_size_ >= alloc_size) {
  //   return;
  // }
  // alloc_size = round_up_align(alloc_size, kPageSize);
  // // Release the previous allocated buffer
  // if (async_buf_) {
  //   assert(!async_read_request_.io_req.IsPending());
  //   free(async_buf_);
  //   async_buf_ = nullptr;
  //   async_buf_size_ = 0;
  // }
  // posix_memalign((void**)&async_buf_, kPageSize, alloc_size);
  // if (async_buf_) {
  //   async_buf_size_ = alloc_size;
  // }
}

size_t ZonedRandomAccessFile::GetUniqueId(char* id, size_t max_size) const {
  return zoneFile_->GetUniqueId(id, max_size);
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && !defined(OS_WIN)
