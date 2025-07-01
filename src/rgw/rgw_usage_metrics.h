#ifndef RGW_USAGE_METRICS_H
#define RGW_USAGE_METRICS_H

#include <string>
#include <thread>
#include <atomic>
#include <lmdb.h>
#include "common/ceph_context.h"
#include "rgw_perf_counters.h"
#include "common/perf_counters_cache.h"
#include "rgw_sal_fwd.h"

namespace rgw {

class UsageMetrics {
  CephContext *cct{nullptr};
  rgw::sal::Driver *driver{nullptr};
  MDB_env *env{nullptr};
  MDB_dbi dbi{0};
  std::thread thr;
  std::atomic<bool> stop_flag{false};
  PerfCountersCache *user_cache{nullptr};
  PerfCountersCache *bucket_cache{nullptr};
  uint64_t interval{60};

  void background();
  void load_from_db();
  void update_counter(const std::string& key, uint64_t bytes, uint64_t objs);
public:
  explicit UsageMetrics(CephContext *cct);
  ~UsageMetrics();

  int start(rgw::sal::Driver *driver);
  void stop();
};

} // namespace rgw

#endif
