#pragma once

#include <libaio.h>

#include <cassert>
#include <cstdint>
#include <cstring>

#include "rocksdb/io_status.h"
#include "rocksdb/status.h"

namespace TERARKDB_NAMESPACE {
// A wrapper for Linux AsyncIO request sending and checking. This struct 
// only supports one async io request currently. The user has to organize 
// multiple async io requests in their own way
struct AsyncIORequest {
  iocb cb;
  iocb* cb_ptr[1] = {&cb};
  io_context_t ctx;
  io_event event;
  bool pending_async = false;  // indicate if this request is pending

  // Initialize context of this Async request
  bool Init() {
    pending_async = false;
    std::memset(&ctx, 0, sizeof(io_context_t));
    auto ret = io_setup(1, &ctx);
    return ret == 0;
  }

  // Initialize an async read command
  void PrepareRead(int fd, size_t sz, uint64_t off, char* buf) {
    io_prep_pread(&cb, fd, buf, sz, off);
  }

  // Initialize an async write command
  void PrepareWrite(int fd, size_t sz, uint64_t off, char* buf) {
    io_prep_pwrite(&cb, fd, buf, sz, off);
  }

  // Submit this async command. Return false if any error happens
  // Set the pending flag.
  IOStatus Submit() {
    auto ret = io_submit(ctx, 1, cb_ptr);
    if (ret < 0) {
      return IOStatus::IOError("AsyncIORequest Submit error");
    }
    pending_async = true;
    return IOStatus::OK();
  }

  IOStatus CheckFinish() {
    assert(IsPending());
    timespec timeout;
    timeout.tv_sec = 0;
    // Check for 1ms
    timeout.tv_nsec = 1000000;
    if (io_getevents(ctx, 0, 1, &event, &timeout) == 1) {
      return IOStatus::OK();
    }
    return IOStatus::AsyncError("AsyncIORequest Incomplete");
  }

  bool IsPending() const { return pending_async; }
};
}  // namespace TERARKDB_NAMESPACE