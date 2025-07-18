#ifdef LINEAGE
#include "duckdb/execution/lineage/log_data.hpp"

namespace duckdb {

  std::pair<int, int> Log::LatestLSN() const {
    return latest;
  }

  void Log::SetLatestLSN(const std::pair<int, int>& lsn_fn) {
    latest = lsn_fn;
  }

  Log::~Log() {
    for (int i=0; i < int_scatter_log.size(); ++i) {
      delete int_scatter_log[i].addresses;
    }
    for (int i=0; i < scatter_log.size(); ++i) {
      delete scatter_log[i].addresses;
    }
    for (int i=0; i < filter_log.size(); ++i) {
      delete filter_log[i].sel;
    }
    for (int i=0; i < row_group_log.size(); ++i) {
      delete row_group_log[i].sel;
    }
    for (int i=0; i < bnlj_log.size(); ++i) {
      delete bnlj_log[i].sel;
    }
    for (int i=0; i < join_gather_log.size(); ++i) {
      delete join_gather_log[i].rhs;
      delete join_gather_log[i].lhs;
    }
    for (int i=0; i < scatter_sel_log.size(); ++i) {
      delete scatter_sel_log[i].addresses;
      delete scatter_sel_log[i].sel;
    }
    for (int i=0; i < perfect_probe_ht_log.size(); ++i) {
      delete perfect_probe_ht_log[i].right;
      delete perfect_probe_ht_log[i].left;
    }
    
    for (int i=0; i < finalize_states_log.size(); ++i) {
      delete finalize_states_log[i].addresses;
    }
    
    for (int i=0; i < combine_log.size(); ++i) {
      delete combine_log[i].src;
      delete combine_log[i].target;
    }
  }
} // namespace duckdb
#endif
