/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <fcntl.h>
#include <pthread.h>

#include "backoff.h"
#include "cache_posix.h"
#include "crypto/hash.h"
#include "fetch.h"
#include "network/download.h"
#include "statistics.h"
#include "testutil.h"
#include "util/atomic.h"

using namespace std;  // NOLINT

namespace cvmfs {

class T_Fetcher : public ::testing::Test {
 protected:
  virtual void SetUp() {
    used_fds_ = GetNoUsedFds();

    tmp_path_ = CreateTempDir(GetCurrentWorkingDirectory() + "/cvmfs_ut_fetch");
    // 4 source files that can be downloaded
    src_path_ = tmp_path_ + "/data";
    hash_regular_ = shash::Any(shash::kSha1);
    hash_uncompressed_ = shash::Any(shash::kSha1);
    hash_catalog_ = shash::Any(shash::kSha1, shash::kSuffixCatalog);
    hash_cert_ = shash::Any(shash::kSha1, shash::kSuffixCertificate);
    unsigned char x = 'x';
    unsigned char y = 'y';
    unsigned char z = 'z';
    void *buf;
    uint64_t buf_size;
    EXPECT_TRUE(zlib::CompressMem2Mem(&x, 1, &buf, &buf_size));
    shash::HashMem(static_cast<unsigned char *>(buf), buf_size, &hash_regular_);
    shash::HashMem(&x, 1, &hash_uncompressed_);
    MkdirDeep(GetParentPath(src_path_ + "/" + hash_regular_.MakePath()), 0700);
    MkdirDeep(GetParentPath(src_path_ + "/" + hash_uncompressed_.MakePath()),
              0700);
    EXPECT_TRUE(CopyMem2Path(static_cast<unsigned char *>(buf), buf_size,
                             src_path_ + "/" + hash_regular_.MakePath()));
    EXPECT_TRUE(CopyMem2Path(&x, 1,
                             src_path_ + "/" + hash_uncompressed_.MakePath()));
    EXPECT_TRUE(CopyMem2Path(static_cast<unsigned char *>(buf), buf_size,
                             tmp_path_ + "/reg"));
    EXPECT_TRUE(CopyMem2Path(static_cast<unsigned char *>(buf), buf_size,
                             tmp_path_ + "/altpath"));
    free(buf);
    EXPECT_TRUE(zlib::CompressMem2Mem(&y, 1, &buf, &buf_size));
    shash::HashMem(static_cast<unsigned char *>(buf), buf_size, &hash_catalog_);
    MkdirDeep(GetParentPath(src_path_ + "/" + hash_catalog_.MakePath()), 0700);
    EXPECT_TRUE(CopyMem2Path(static_cast<unsigned char *>(buf), buf_size,
                             src_path_ + "/" + hash_catalog_.MakePath()));
    free(buf);
    EXPECT_TRUE(zlib::CompressMem2Mem(&z, 1, &buf, &buf_size));
    shash::HashMem(static_cast<unsigned char *>(buf), buf_size, &hash_cert_);
    MkdirDeep(GetParentPath(src_path_ + "/" + hash_cert_.MakePath()), 0700);
    EXPECT_TRUE(CopyMem2Path(static_cast<unsigned char *>(buf), buf_size,
                             src_path_ + "/" + hash_cert_.MakePath()));
    free(buf);


    cache_mgr_ = PosixCacheManager::Create(tmp_path_, false);
    ASSERT_TRUE(cache_mgr_ != NULL);

    download_mgr_ = new download::DownloadManager();
    download_mgr_->Init(8,
      perf::StatisticsTemplate("test", &statistics_));
    download_mgr_->SetHostChain("file://" + tmp_path_);

    fetcher_ = new Fetcher(
      cache_mgr_, download_mgr_, &backoff_throttle_,
      perf::StatisticsTemplate("fetch", &statistics_));
    external_fetcher_ = new Fetcher(
      cache_mgr_, download_mgr_, &backoff_throttle_,
      perf::StatisticsTemplate("fetch-external", &statistics_));
  }

  virtual void TearDown() {
    delete fetcher_;
    delete external_fetcher_;
    download_mgr_->Fini();
    delete download_mgr_;
    delete cache_mgr_;
    if (tmp_path_ != "")
      RemoveTree(tmp_path_);
    EXPECT_EQ(used_fds_, GetNoUsedFds());
  }

  Fetcher *fetcher_;
  Fetcher *external_fetcher_;
  PosixCacheManager *cache_mgr_;
  perf::Statistics statistics_;
  download::DownloadManager *download_mgr_;
  unsigned used_fds_;
  shash::Any hash_regular_;
  shash::Any hash_uncompressed_;
  shash::Any hash_catalog_;
  shash::Any hash_cert_;
  string tmp_path_;
  string src_path_;
  BackoffThrottle backoff_throttle_;
};


/**
 * Fails sometimes...
 */
class BuggyCacheManager : public CacheManager {
 public:
  BuggyCacheManager()
    : open_2nd_try(false)
    , allow_open(false)
    , stall_in_ctrltxn(false)
    , allow_open_from_txn(false)
  {
    atomic_init32(&waiting_in_ctrltxn);
    atomic_init32(&continue_ctrltxn);
  }
  virtual CacheManagerIds id() { return kUnknownCacheManager; }
  virtual std::string Describe() { return "test\n"; }
  virtual bool AcquireQuotaManager(QuotaManager *qm) { return false; }
  virtual int Open(const LabeledObject & /* object */) {
    if (!allow_open) {
      if (open_2nd_try)
        allow_open = true;
      return -ENOENT;
    } else {
      return open("/dev/null", O_RDONLY);
    }
  }
  virtual int64_t GetSize(int fd) { return 0; }
  virtual int Close(int fd) { return close(fd); }
  virtual int64_t Pread(int fd, void *buf, uint64_t size, uint64_t offset) {
    return -EIO;
  }
  virtual int Dup(int fd) { return -EROFS; }
  virtual int Readahead(int fd) { return 0; }
  virtual uint32_t SizeOfTxn() { return sizeof(int); }
  virtual int StartTxn(const shash::Any &id, uint64_t size, void *txn) {
    int fd = open("/dev/null", O_RDONLY);
    assert(fd >= 0);
    *static_cast<int *>(txn) = fd;
    return 0;
  }
  virtual void CtrlTxn(const Label & /* label */, const int /* flags */,
                       void * /* txn */)
  {
    if (stall_in_ctrltxn) {
      atomic_inc32(&waiting_in_ctrltxn);
      while (atomic_read32(&continue_ctrltxn) == 0) { }
      atomic_dec32(&waiting_in_ctrltxn);
    }
  }
  virtual int64_t Write(const void *buf, uint64_t sz, void *txn) {
    return sz;
  }
  virtual int Reset(void *txn) { return 0; }
  virtual int AbortTxn(void *txn) {
    return close(*static_cast<int *>(txn));
  }
  virtual int OpenFromTxn(void *txn) {
    if (allow_open_from_txn) {
      return dup(*static_cast<int *>(txn));
    } else {
      return -EBADF;
    }
  }
  virtual int CommitTxn(void *txn) {
    return close(*static_cast<int *>(txn));
  }
  virtual void Spawn() { }

  bool open_2nd_try;
  bool allow_open;
  bool stall_in_ctrltxn;
  atomic_int32 waiting_in_ctrltxn;
  atomic_int32 continue_ctrltxn;
  bool allow_open_from_txn;
};


void *TestGetTls(void *data) {
  Fetcher *f = static_cast<Fetcher *>(data);
  void *thread_tls = f->GetTls();
  EXPECT_TRUE(thread_tls != NULL);
  EXPECT_EQ(thread_tls, f->GetTls());
  EXPECT_EQ(2U, f->tls_blocks_.size());
  return thread_tls;
}

TEST_F(T_Fetcher, GetTls) {
  void *this_tls = fetcher_->GetTls();
  EXPECT_TRUE(this_tls != NULL);
  // Idempotent
  EXPECT_EQ(this_tls, fetcher_->GetTls());
  EXPECT_EQ(1U, fetcher_->tls_blocks_.size());

  pthread_t thread;
  EXPECT_EQ(0, pthread_create(&thread, NULL, TestGetTls, fetcher_));
  void *other_thread_tls;
  pthread_join(thread, &other_thread_tls);
  EXPECT_TRUE(other_thread_tls != NULL);
  EXPECT_NE(other_thread_tls, this_tls);
}


TEST_F(T_Fetcher, ExternalFetch) {
  // Make sure our file is not in the cache
  EXPECT_EQ(0, unlink((src_path_ + "/" + hash_regular_.MakePath()).c_str()));

  CacheManager::Label lbl;
  lbl.flags |= CacheManager::kLabelExternal;

  // Download fails
  lbl.path = "/reg-fail";

  EXPECT_EQ(-EIO,
    external_fetcher_->Fetch(CacheManager::LabeledObject(hash_regular_, lbl)));

  // Download and store in cache
  lbl.path = "/reg";
  int fd =
    external_fetcher_->Fetch(CacheManager::LabeledObject(hash_regular_, lbl));
  EXPECT_GE(fd, 0);
  EXPECT_EQ(0, cache_mgr_->Close(fd));
  fd = cache_mgr_->Open(CacheManager::LabeledObject(hash_regular_));
  EXPECT_GE(fd, 0);
  EXPECT_EQ(0, cache_mgr_->Close(fd));

  // Download fails
  shash::Any rnd_hash(shash::kSha1);
  rnd_hash.Randomize();
  EXPECT_EQ(-EIO,
    fetcher_->Fetch(CacheManager::LabeledObject(rnd_hash, lbl)));
}


TEST_F(T_Fetcher, Fetch) {
  // Cache hit
  unsigned char x = 'x';
  shash::Any hash_avail(shash::kSha1);
  EXPECT_TRUE(cache_mgr_->CommitFromMem(CacheManager::LabeledObject(hash_avail),
                                        &x, 1));
  CacheManager::Label lbl;
  lbl.size = 1;
  int fd = fetcher_->Fetch(CacheManager::LabeledObject(hash_avail, lbl));
  EXPECT_GE(fd, 0);
  EXPECT_EQ(0, cache_mgr_->Close(fd));
  lbl.flags = CacheManager::kLabelCatalog;
  fd = fetcher_->Fetch(CacheManager::LabeledObject(hash_avail, lbl));
  EXPECT_GE(fd, 0);
  EXPECT_EQ(0, cache_mgr_->Close(fd));

  // Download and store in cache
  lbl.flags = 0;
  lbl.path = "reg";
  lbl.size = CacheManager::kSizeUnknown;
  fd = fetcher_->Fetch(CacheManager::LabeledObject(hash_regular_, lbl));
  EXPECT_GE(fd, 0);
  EXPECT_EQ(0, cache_mgr_->Close(fd));
  fd = cache_mgr_->Open(CacheManager::LabeledObject(hash_regular_, lbl));
  EXPECT_GE(fd, 0);
  EXPECT_EQ(0, cache_mgr_->Close(fd));

  // Download fails
  shash::Any rnd_hash(shash::kSha1);
  rnd_hash.Randomize();
  lbl.path = "rnd";
  EXPECT_EQ(-EIO, fetcher_->Fetch(CacheManager::LabeledObject(rnd_hash, lbl)));

  // Download and store catalog
  lbl.path = "cat";
  lbl.flags = CacheManager::kLabelCatalog;
  fd = fetcher_->Fetch(CacheManager::LabeledObject(hash_catalog_, lbl));
  EXPECT_GE(fd, 0);
  EXPECT_EQ(0, cache_mgr_->Close(fd));
  fd = cache_mgr_->Open(CacheManager::LabeledObject(hash_catalog_));
  EXPECT_GE(fd, 0);
  EXPECT_EQ(0, cache_mgr_->Close(fd));
}


TEST_F(T_Fetcher, FetchUncompressed) {
  EXPECT_EQ(-ENOENT,
            cache_mgr_->Open(CacheManager::LabeledObject(hash_uncompressed_)));

  // Download and store in cache
  // TODO(jblomer): use CacheManager::kSizeUnknown
  CacheManager::Label lbl;
  lbl.size = 1;
  lbl.path = "x";
  int fd =
    fetcher_->Fetch(CacheManager::LabeledObject(hash_uncompressed_, lbl));
  EXPECT_EQ(-EIO, fd);

  lbl.zip_algorithm = zlib::kNoCompression;
  fd = fetcher_->Fetch(CacheManager::LabeledObject(hash_uncompressed_, lbl));
  EXPECT_GE(fd, 0);
  EXPECT_EQ(0, cache_mgr_->Close(fd));
  fd = cache_mgr_->Open(CacheManager::LabeledObject(hash_uncompressed_));
  EXPECT_GE(fd, 0);
  EXPECT_EQ(0, cache_mgr_->Close(fd));
}


TEST_F(T_Fetcher, FetchAltPath) {
  unlink((src_path_ + "/" + hash_regular_.MakePath()).c_str());
  int fd;
  fd = fetcher_->Fetch(CacheManager::LabeledObject(hash_regular_));
  EXPECT_LT(fd, 0);

  fd = fetcher_->Fetch(CacheManager::LabeledObject(hash_regular_), "altpath");
  EXPECT_GE(fd, 0);
  EXPECT_EQ(0, cache_mgr_->Close(fd));
}


TEST_F(T_Fetcher, FetchTransactionFailures) {
  // OpenFromTxn fails
  perf::Statistics statistics;
  BuggyCacheManager bcm;
  Fetcher f(&bcm, download_mgr_, &backoff_throttle_,
    perf::StatisticsTemplate("fetch", &statistics));
  CacheManager::Label lbl;
  lbl.path = "cat";
  lbl.flags = CacheManager::kLabelCatalog;
  EXPECT_EQ(-EBADF, f.Fetch(CacheManager::LabeledObject(hash_catalog_, lbl)));

  // Wrong size (commit fails)
  lbl.size = 2;
  EXPECT_EQ(-EIO,
    fetcher_->Fetch(CacheManager::LabeledObject(hash_cert_, lbl)));
  EXPECT_TRUE(FileExists(tmp_path_ + "/quarantaine/" + hash_cert_.ToString()));
  lbl.flags = 0;
  lbl.size = 1;
  int fd = fetcher_->Fetch(CacheManager::LabeledObject(hash_cert_, lbl));
  EXPECT_GE(fd, 0);
  EXPECT_EQ(0, cache_mgr_->Close(fd));

  // StartTxn fails
  RemoveTree(tmp_path_ + "/txn");
  lbl.path = "reg";
  lbl.size = CacheManager::kSizeUnknown;
  lbl.flags = 0;
  EXPECT_EQ(-ENOENT,
    fetcher_->Fetch(CacheManager::LabeledObject(hash_regular_, lbl)));
}


struct TestFetchCollapseInfo {
  Fetcher *f;
  shash::Any hash;
};

void *TestFetchCollapse(void *data) {
  TestFetchCollapseInfo *info = reinterpret_cast<TestFetchCollapseInfo *>(data);
  Fetcher *f = info->f;
  BuggyCacheManager *bcm = reinterpret_cast<BuggyCacheManager *>(f->cache_mgr_);
  CacheManager::Label lbl;
  lbl.path = "cat";
  lbl.flags = CacheManager::kLabelCatalog;
  int fd = f->Fetch(CacheManager::LabeledObject(info->hash, lbl));
  EXPECT_GE(fd, 0);
  EXPECT_EQ(0, bcm->Close(fd));
  return NULL;
}

void *TestFetchCollapse2(void *data) {
  Fetcher *f = reinterpret_cast<Fetcher *>(data);
  BuggyCacheManager *bcm = reinterpret_cast<BuggyCacheManager *>(f->cache_mgr_);
  while (!bcm->continue_ctrltxn) {
    pthread_mutex_lock(f->lock_queues_download_);
    Fetcher::ThreadQueues::iterator iDownloadQueue =
      f->queues_download_.begin();
    for (; iDownloadQueue != f->queues_download_.end(); ++iDownloadQueue) {
      if (iDownloadQueue->second->size() > 0) {
        // printf("open up %s", iDownloadQueue->first.ToString().c_str());
        bcm->stall_in_ctrltxn = false;
        atomic_inc32(&bcm->continue_ctrltxn);
      }
    }
    pthread_mutex_unlock(f->lock_queues_download_);
  }

  return NULL;
}

TEST_F(T_Fetcher, FetchCollapse) {
  // Test race condition: first open fails, second one succeeds
  perf::Statistics statistics;
  BuggyCacheManager bcm;
  bcm.open_2nd_try = true;
  Fetcher f(&bcm, download_mgr_, &backoff_throttle_,
    perf::StatisticsTemplate("fetch", &statistics));
  CacheManager::Label lbl;
  lbl.path = "cat";
  lbl.flags = CacheManager::kLabelCatalog;
  int fd = f.Fetch(CacheManager::LabeledObject(hash_catalog_, lbl));
  EXPECT_GE(fd, 0);
  EXPECT_EQ(0, bcm.Close(fd));
  // One again, nothing should be locked
  fd = f.Fetch(CacheManager::LabeledObject(hash_catalog_, lbl));
  EXPECT_GE(fd, 0);
  EXPECT_EQ(0, bcm.Close(fd));

  bcm.open_2nd_try = false;
  bcm.allow_open = false;
  bcm.allow_open_from_txn = true;
  bcm.stall_in_ctrltxn = true;
  pthread_t thread_collapse;
  pthread_t thread_collapse2;
  TestFetchCollapseInfo info;
  info.f = &f;
  info.hash = hash_catalog_;
  EXPECT_EQ(0,
    pthread_create(&thread_collapse, NULL, TestFetchCollapse, &info));
  EXPECT_EQ(0,
    pthread_create(&thread_collapse2, NULL, TestFetchCollapse2, &f));

  // Piggy-back onto existing download
  while (atomic_read32(&bcm.waiting_in_ctrltxn) == 0) { }
  fd = f.Fetch(CacheManager::LabeledObject(hash_catalog_, lbl));
  EXPECT_EQ(-EROFS, fd);
  pthread_join(thread_collapse, NULL);
  pthread_join(thread_collapse2, NULL);
}


TEST_F(T_Fetcher, SignalWaitingThreads) {
  unsigned char x = 'x';
  EXPECT_TRUE(cache_mgr_->CommitFromMem(
    CacheManager::LabeledObject(hash_regular_), &x, 1));
  int fd = cache_mgr_->Open(CacheManager::LabeledObject(hash_regular_));
  EXPECT_GE(fd, 0);
  int tls_pipe[2];
  MakePipe(tls_pipe);

  fetcher_->queues_download_[hash_regular_] = NULL;
  fetcher_->queues_download_[hash_catalog_] = NULL;
  fetcher_->queues_download_[hash_cert_] = NULL;

  fetcher_->GetTls()->other_pipes_waiting.push_back(tls_pipe[1]);
  fetcher_->SignalWaitingThreads(-1, hash_regular_, fetcher_->GetTls());
  EXPECT_EQ(0U, fetcher_->queues_download_.count(hash_regular_));

  fetcher_->GetTls()->other_pipes_waiting.push_back(tls_pipe[1]);
  fetcher_->SignalWaitingThreads(fd, hash_catalog_, fetcher_->GetTls());
  EXPECT_EQ(0U, fetcher_->queues_download_.count(hash_catalog_));

  fetcher_->GetTls()->other_pipes_waiting.push_back(tls_pipe[1]);
  fetcher_->SignalWaitingThreads(1000000, hash_cert_, fetcher_->GetTls());
  EXPECT_EQ(0U, fetcher_->queues_download_.count(hash_cert_));

  int fd_return0;
  int fd_return1;
  int fd_return2;
  ReadPipe(tls_pipe[0], &fd_return0, sizeof(fd_return0));
  ReadPipe(tls_pipe[0], &fd_return1, sizeof(fd_return1));
  ReadPipe(tls_pipe[0], &fd_return2, sizeof(fd_return2));
  EXPECT_EQ(-1, fd_return0);
  EXPECT_NE(fd, fd_return1);
  EXPECT_EQ(0, cache_mgr_->Close(fd_return1));
  EXPECT_EQ(-EBADF, fd_return2);

  ClosePipe(tls_pipe);
  EXPECT_EQ(0, cache_mgr_->Close(fd));
}

}  // namespace cvmfs
