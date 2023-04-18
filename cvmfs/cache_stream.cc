/**
 * This file is part of the CernVM File System.
 */

#include "cvmfs_config.h"
#include "cache_stream.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <string>

#include "network/download.h"
#include "network/sink.h"
#include "util/mutex.h"
#include "util/smalloc.h"


namespace {

class StreamingSink : public cvmfs::Sink {
 public:
  StreamingSink(void *buf, uint64_t size, uint64_t offset)
    : pos_(0)
    , window_buf_(buf)
    , window_size_(size)
    , window_offset_(offset)
  { }

  virtual ~StreamingSink() {}

  virtual int64_t Write(const void *buf, uint64_t sz) {
    uint64_t old_pos = pos_;
    pos_ += sz;

    if (!window_buf_)
      return sz;

    if (pos_ < window_offset_)
      return sz;

    if (old_pos >= (window_offset_ + window_size_))
      return sz;

    uint64_t copy_offset = std::max(old_pos, window_offset_);
    uint64_t inbuf_offset = copy_offset - old_pos;
    uint64_t outbuf_offset = copy_offset - window_offset_;
    uint64_t copy_size =
      std::min(sz - inbuf_offset, window_size_ - outbuf_offset);

    memcpy(reinterpret_cast<unsigned char *>(window_buf_) + outbuf_offset,
           reinterpret_cast<const unsigned char *>(buf) + inbuf_offset,
           copy_size);

    return sz;
  }

  virtual int Reset() {
    pos_ = 0;
    return 0;
  }

  int64_t GetNBytesWritten() const { return pos_; }

 private:
   uint64_t pos_;
   void *window_buf_;
   uint64_t window_size_;
   uint64_t window_offset_;
};  // class StreamingSink

}  // anonymous namespace


int64_t StreamingCacheManager::Stream(
  const shash::Any &object_id,
  void *buf,
  uint64_t size,
  uint64_t offset)
{
  StreamingSink sink(buf, size, offset);
  std::string url = "data/" + object_id.MakePath();
  download::JobInfo download_job(&url,
                                 true, /* compressed */
                                 true, /* probe_hosts */
                                 &sink,
                                 &object_id);
  download_mgr_->Fetch(&download_job);

  if (download_job.error_code != download::kFailOk)
    return -EIO;

  return sink.GetNBytesWritten();
}


StreamingCacheManager::StreamingCacheManager(
  unsigned max_open_fds,
  CacheManager *cache_mgr,
  download::DownloadManager *download_mgr)
  : cache_mgr_(cache_mgr)
  , download_mgr_(download_mgr)
  , fd_table_(max_open_fds, FdInfo())
{
  lock_fd_table_ =
    reinterpret_cast<pthread_mutex_t *>(smalloc(sizeof(pthread_mutex_t)));
  int retval = pthread_mutex_init(lock_fd_table_, NULL);
  assert(retval == 0);
}

StreamingCacheManager::~StreamingCacheManager() {
  pthread_mutex_destroy(lock_fd_table_);
  free(lock_fd_table_);
  quota_mgr_ = NULL;  // gets deleted by cache_mgr_
}

std::string StreamingCacheManager::Describe() {
  return "Streaming shim, underlying cache manager:\n" + cache_mgr_->Describe();
}

bool StreamingCacheManager::AcquireQuotaManager(QuotaManager *quota_mgr) {
  bool result = cache_mgr_->AcquireQuotaManager(quota_mgr);
  if (result)
    quota_mgr_ = cache_mgr_->quota_mgr();
  return result;
}

int StreamingCacheManager::Open(const BlessedObject &object) {
  int fd_in_cache_mgr = cache_mgr_->Open(object);
  if (fd_in_cache_mgr >= 0) {
    MutexLockGuard lock_guard(lock_fd_table_);
    return fd_table_.OpenFd(FdInfo(fd_in_cache_mgr));
  }

  if (fd_in_cache_mgr != -ENOENT)
    return fd_in_cache_mgr;

  if ((object.info.type == kTypeCatalog) || (object.info.type == kTypePinned))
    return -ENOENT;

  MutexLockGuard lock_guard(lock_fd_table_);
  return fd_table_.OpenFd(FdInfo(object.id));
}

int64_t StreamingCacheManager::GetSize(int fd) {
  FdInfo info;
  {
    MutexLockGuard lock_guard(lock_fd_table_);
    info = fd_table_.GetHandle(fd);
  }

  if (!info.IsValid())
    return -EBADF;

  if (info.fd_in_cache_mgr >= 0)
    return cache_mgr_->GetSize(info.fd_in_cache_mgr);

  return Stream(info.object_id, NULL, 0, 0);
}

int StreamingCacheManager::Dup(int fd) {
  FdInfo info;

  MutexLockGuard lock_guard(lock_fd_table_);
  info = fd_table_.GetHandle(fd);

  if (!info.IsValid())
    return -EBADF;

  if (info.fd_in_cache_mgr >= 0) {
    int dup_fd = cache_mgr_->Dup(info.fd_in_cache_mgr);
    if (dup_fd < 0)
      return dup_fd;
    return fd_table_.OpenFd(FdInfo(dup_fd));
  }

  return fd_table_.OpenFd(FdInfo(info.object_id));
}

int StreamingCacheManager::Close(int fd) {
  FdInfo info;
  {
    MutexLockGuard lock_guard(lock_fd_table_);
    info = fd_table_.GetHandle(fd);
    if (!info.IsValid())
      return -EBADF;
    fd_table_.CloseFd(fd);
  }

  if (info.fd_in_cache_mgr >= 0)
    return cache_mgr_->Close(info.fd_in_cache_mgr);

  return 0;
}

int64_t StreamingCacheManager::Pread(
  int fd, void *buf, uint64_t size, uint64_t offset)
{
  FdInfo info;
  {
    MutexLockGuard lock_guard(lock_fd_table_);
    info = fd_table_.GetHandle(fd);
  }

  if (!info.IsValid())
    return -EBADF;

  if (info.fd_in_cache_mgr >= 0)
    return cache_mgr_->Pread(info.fd_in_cache_mgr, buf, size, offset);

  return Stream(info.object_id, buf, size, offset);
}

int StreamingCacheManager::Readahead(int fd) {
  FdInfo info;
  {
    MutexLockGuard lock_guard(lock_fd_table_);
    info = fd_table_.GetHandle(fd);
  }

  if (!info.IsValid())
    return -EBADF;

  if (info.fd_in_cache_mgr >= 0)
    return cache_mgr_->Readahead(info.fd_in_cache_mgr);

  return 0;
}

int StreamingCacheManager::OpenFromTxn(void *txn) {
  int fd = cache_mgr_->OpenFromTxn(txn);
  if (fd < 0)
    return fd;

  MutexLockGuard lock_guard(lock_fd_table_);
  return fd_table_.OpenFd(FdInfo(fd));
}
