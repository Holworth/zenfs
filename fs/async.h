#pragma once

#include <libaio.h>

#include <cassert>
#include <cstdint>
#include <cstring>
#include <string>

#include "fs/log.h"
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
  IOStatus Init() {
    pending_async = false;
    std::memset(&ctx, 0, sizeof(io_context_t));
    auto ret = io_setup(1, &ctx);
    if (ret != 0) {
      return IOStatus::AsyncError("io_setup error: " +
                                  std::string(strerror(errno)));
    }
    return IOStatus::OK();
  }

  // Abort this request
  IOStatus Cancel() {
    assert(IsPending());
    auto ret = io_cancel(ctx, &cb, nullptr);
    if (ret < 0) {
      return IOStatus::AsyncError("Cancel aio request failed: " +
                                  std::string(strerror(errno)));
    }
    return IOStatus::OK();
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
    // io_submit returns the number of requests successfully queued
    if (ret != 1) {
      if (ret < 0) {
        return IOStatus::AsyncError("AsyncIORequest Submit error: " +
                                    std::string(strerror(errno)));
      }
      return IOStatus::AsyncError("AsyncIORequest Submit error: Only submit: " +
                                  std::to_string(ret));
    }
    pending_async = true;
    return IOStatus::OK();
  }

  IOStatus CheckFinish() {
    assert(IsPending());
    timespec timeout;
    timeout.tv_sec = 0;
    // Check for 1ms
    timeout.tv_nsec = 1000;
    if (io_getevents(ctx, 0, 1, &event, &timeout) == 1) {
      return IOStatus::OK();
    }
    return IOStatus::AsyncError("AsyncIORequest Incomplete");
  }

  bool IsPending() const { return pending_async; }
};
}  // namespace TERARKDB_NAMESPACE