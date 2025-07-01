#include "rgw_usage_metrics.h"
#include "common/dout.h"
#include "common/perf_counters.h"
#include "rgw_sal.h"

#define dout_subsys ceph_subsys_rgw

namespace rgw {

static std::shared_ptr<PerfCounters> create_usage_counters(const std::string& name, CephContext* cct) {
  PerfCountersBuilder pcb(cct, name, l_rgw_usage_first, l_rgw_usage_last);
  pcb.set_prio_default(PerfCountersBuilder::PRIO_USEFUL);
  pcb.add_u64_counter(l_rgw_usage_used_bytes, "used_bytes", "Used bytes");
  pcb.add_u64_counter(l_rgw_usage_num_objects, "num_objects", "Number of objects");
  auto pc = pcb.create_perf_counters();
  cct->get_perfcounters_collection()->add(pc);
  return std::shared_ptr<PerfCounters>(pc);
}

UsageMetrics::UsageMetrics(CephContext *cct) : cct(cct) {}

UsageMetrics::~UsageMetrics() {
  stop();
}

int UsageMetrics::start(rgw::sal::Driver *d) {
  driver = d;
  interval = cct->_conf->get_val<uint64_t>("rgw_usage_metrics_refresh_interval");
  std::string path = cct->_conf->get_val<std::string>("rgw_usage_metrics_db_path");
  int r = mdb_env_create(&env);
  if (r == 0) r = mdb_env_set_maxdbs(env, 1);
  if (r == 0) r = mdb_env_open(env, path.c_str(), 0, 0664);
  if (r != 0) {
    ldout(cct, 1) << "failed to open usage lmdb: " << cpp_strerror(r) << dendl;
    if (env) mdb_env_close(env);
    env = nullptr;
    return -r;
  }
  MDB_txn *txn;
  r = mdb_txn_begin(env, nullptr, 0, &txn);
  if (r == 0) {
    r = mdb_dbi_open(txn, "usage", MDB_CREATE, &dbi);
    if (r == 0)
      r = mdb_txn_commit(txn);
    else
      mdb_txn_abort(txn);
  }
  if (r != 0) {
    ldout(cct, 1) << "failed to open usage db: " << cpp_strerror(r) << dendl;
    mdb_env_close(env);
    env = nullptr;
    return -r;
  }

  user_cache = new PerfCountersCache(cct, 1024, create_usage_counters);
  bucket_cache = new PerfCountersCache(cct, 1024, create_usage_counters);

  stop_flag = false;
  thr = std::thread(&UsageMetrics::background, this);
  return 0;
}

void UsageMetrics::stop() {
  stop_flag = true;
  if (thr.joinable())
    thr.join();
  if (env) {
    mdb_env_close(env);
    env = nullptr;
  }
  delete user_cache;
  user_cache = nullptr;
  delete bucket_cache;
  bucket_cache = nullptr;
}

void UsageMetrics::update_counter(const std::string& key, uint64_t bytes, uint64_t objs) {
  PerfCountersCache *cache = key.rfind("user:",0) == 0 ? user_cache : bucket_cache;
  if (cache) {
    cache->set_counter(key, l_rgw_usage_used_bytes, bytes);
    cache->set_counter(key, l_rgw_usage_num_objects, objs);
  }
}

void UsageMetrics::load_from_db() {
  if (!env) return;
  MDB_txn *txn;
  if (mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn) != 0)
    return;
  MDB_cursor *cursor;
  if (mdb_cursor_open(txn, dbi, &cursor) == 0) {
    MDB_val k, v;
    while (mdb_cursor_get(cursor, &k, &v, MDB_NEXT) == 0) {
      std::string key(static_cast<char*>(k.mv_data), k.mv_size);
      if (v.mv_size == sizeof(uint64_t)*2) {
        const uint64_t* vals = static_cast<const uint64_t*>(v.mv_data);
        update_counter(key, vals[0], vals[1]);
      }
    }
    mdb_cursor_close(cursor);
  }
  mdb_txn_abort(txn);
}

void UsageMetrics::background() {
  ceph_pthread_setname("usage_metrics");
  while (!stop_flag) {
    load_from_db();
    sleep(interval);
  }
}

} // namespace rgw
