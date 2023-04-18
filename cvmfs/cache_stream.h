/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_CACHE_STREAM_H_
#define CVMFS_CACHE_STREAM_H_

#include "cache.h"

#include <pthread.h>

#include <string>

#include <crypto/hash.h>
#include <fd_table.h>
#include <util/pointer.h>

namespace download {
class DownloadManager;
}

/**
 * Cache manager that streams regular files using a download manager and stores
 * file catalogs in an underlying cache manager.
 */
class StreamingCacheManager : public CacheManager {
 public:
  StreamingCacheManager(unsigned max_open_fds,
                        CacheManager *cache_mgr,
                        download::DownloadManager *download_mgr);
  virtual ~StreamingCacheManager();

  // In the files system / mountpoint initialization, we create the cache
  // manager before we know about the download manager.  Hence we allow to
  // patch in the download manager at a later point.
  void SetDownloadManager(download::DownloadManager *download_mgr) {
    download_mgr_ = download_mgr;
  }

  virtual CacheManagerIds id() { return kStreamingCacheManager; }
  virtual std::string Describe();

  virtual bool AcquireQuotaManager(QuotaManager *quota_mgr);

  virtual int Open(const BlessedObject &object);
  virtual int64_t GetSize(int fd);
  virtual int Close(int fd);
  virtual int64_t Pread(int fd, void *buf, uint64_t size, uint64_t offset);
  virtual int Dup(int fd);
  virtual int Readahead(int fd);

  // Only pinned objects and catalogs are written to the cache. Transactions
  // are passed through to the backing cache manager.
  virtual uint32_t SizeOfTxn() { return cache_mgr_->SizeOfTxn(); }
  virtual int StartTxn(const shash::Any &id, uint64_t size, void *txn) {
    return cache_mgr_->StartTxn(id, size, txn);
  }
  virtual void CtrlTxn(const ObjectInfo &object_info,
                       const int flags,
                       void *txn)
  {
    cache_mgr_->CtrlTxn(object_info, flags, txn);
  }
  virtual int64_t Write(const void *buf, uint64_t size, void *txn)
  {
    return cache_mgr_->Write(buf, size, txn);
  }
  virtual int Reset(void *txn) { return cache_mgr_->Reset(txn); }
  virtual int OpenFromTxn(void *txn);
  virtual int AbortTxn(void *txn) { return cache_mgr_->AbortTxn(txn); }
  virtual int CommitTxn(void *txn) { return cache_mgr_->CommitTxn(txn); }

  virtual void Spawn() { cache_mgr_->Spawn(); }

  virtual manifest::Breadcrumb LoadBreadcrumb(const std::string &fqrn) {
    return cache_mgr_->LoadBreadcrumb(fqrn);
  }
  virtual bool StoreBreadcrumb(const manifest::Manifest &manifest) {
    return cache_mgr_->StoreBreadcrumb(manifest);
  }

 protected:

 private:
  struct FdInfo {
    int fd_in_cache_mgr;
    shash::Any object_id;

    FdInfo() : fd_in_cache_mgr(-1) {}
    explicit FdInfo(int fd) : fd_in_cache_mgr(fd) {}
    explicit FdInfo(const shash::Any &id)
      : fd_in_cache_mgr(-1), object_id(id) {}

    bool operator ==(const FdInfo &other) const {
      return this->fd_in_cache_mgr == other.fd_in_cache_mgr &&
             this->object_id == other.object_id;
    }
    bool operator !=(const FdInfo &other) const {
      return !(*this == other);
    }

    bool IsValid() const { return fd_in_cache_mgr >= 0 || !object_id.IsNull(); }
  };

  /// Streams an object using the download manager. The complete object is read
  /// and its size is returned (-errno on error).
  /// The given section of the object is copied into the provided buffer,
  /// which may be NULL if only the size of the object is relevant.
  int64_t Stream(const shash::Any &object_id,
                 void *buf, uint64_t size, uint64_t offset);

  UniquePtr<CacheManager> cache_mgr_;
  download::DownloadManager *download_mgr_;

  pthread_mutex_t *lock_fd_table_;
  FdTable<FdInfo> fd_table_;
};  // class StreamingCacheManager

#endif  // CVMFS_CACHE_STREAM_H_
