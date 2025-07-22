#ifdef LINEAGE

#include "duckdb/execution/lineage/operator_lineage.hpp"

namespace duckdb {

shared_ptr<OperatorLineage> GetNextChild(shared_ptr<OperatorLineage>& lop) {
  if (!lop || lop->children.size() == 0) return lop;
  while (lop->type == PhysicalOperatorType::PROJECTION) {
    lop = lop->children[0];
  }
  return lop;
}

void OperatorLineage::LQ(unordered_map<idx_t, vector<idx_t>>& log_context,
                                vector<vector<idx_t>>& iids) {
  for (auto& e : log_context) {
    idx_t pk = e.first;
    idx_t max_threads = thread_vec.size();
    idx_t thread_idx = pk % (max_threads + 1);
    idx_t lsn = pk / (max_threads + 1);
    vector<idx_t>& oids = e.second;
    void* thread_val  = thread_vec[thread_idx];
    auto& tlog = log[thread_val];
    
    bool debug = true;
    if (debug) std::cout << name << " LQ: tid(" << thread_idx << "), lsn(" << lsn << ")," << " |oids|: " << oids.size() << std::endl;

    switch (type) {
    case PhysicalOperatorType::UNGROUPED_AGGREGATE: {
      std::cout << "TODO: handle aggs" << std::endl;
      break;
    } case PhysicalOperatorType::FILTER: {
      D_ASSERT(lsn < tlog->execute_internal.size());
      int blsn = tlog->execute_internal[lsn].first-1;
      int branch = tlog->execute_internal[lsn].second;
      // 1:1 mapping update in place
      if (branch == 0) {
        for (idx_t o=0; o < oids.size(); ++o) {
          idx_t oid = oids[o];
          oids[o] = tlog->filter_log[blsn].sel[oid];
        }
      } else {
        for (idx_t o=0; o < oids.size(); ++o) {
          idx_t oid = oids[o];
          oids[o] = oid;
        }
      }
      // TODO: log->filter_log[lsn].in_lsn and use the same thread_id. not the same as lsn since not all input results in output
      idx_t in_lsn = lsn;
      unordered_map<idx_t, vector<idx_t>> oids_per_lsn;
      idx_t in_pk = in_lsn * (thread_vec.size() + 1) + thread_idx;
      oids_per_lsn[in_pk] = std::move(oids);
      GetNextChild(children[0])->LQ( oids_per_lsn, iids );
      break;
      // TODO: return  LQ(log_context);
    } case PhysicalOperatorType::ORDER_BY: {
      D_ASSERT(lsn < tlog->reorder_log.size());
      if (debug)  std::cout << "single int: " << tlog->single_int_log.size() << std::endl;
      unordered_map<idx_t, vector<idx_t>> oids_per_lsn;
      // TODO: replace with an index
      idx_t offset = 0;
      for (idx_t blsn=0; blsn  < tlog->single_int_log.size(); ++blsn) {
        idx_t pk = blsn * (max_threads + 1) + thread_idx;
        idx_t count = tlog->single_int_log[blsn];
        for (idx_t o=0; o < oids.size(); ++o) {
          idx_t oid = oids[o];
          if (oid >= offset && oid < offset + count) {
            std::cout << count << " add : " << tlog->reorder_log[lsn][oid] - offset << " " << offset << std::endl;
            oids_per_lsn[pk].push_back(tlog->reorder_log[lsn][oid] - offset);
          }
        }
        offset += count;
      }
      GetNextChild(children[0])->LQ( oids_per_lsn, iids );
      break;
    } case PhysicalOperatorType::DELIM_SCAN: {
      std::cout << "TODO: handle delim scan " << std::endl;
      break;
    } case PhysicalOperatorType::COLUMN_DATA_SCAN: {
      // todo: get partition offsets
      iids.emplace_back(std::move(oids));
      break;
    } case PhysicalOperatorType::TABLE_SCAN: { // 1:1 in place update
      D_ASSERT(lsn < tlog->row_group_log.size());
      int count = tlog->row_group_log[lsn].count;
      idx_t offset = tlog->row_group_log[lsn].start + tlog->row_group_log[lsn].vector_index;
      if (tlog->row_group_log[lsn].sel) {
        for (idx_t o=0; o < oids.size(); ++o) {
          idx_t oid = oids[o];
          oids[o] = tlog->row_group_log[lsn].sel[oid] + offset;
        }
      } else {
        for (idx_t o=0; o < oids.size(); ++o) {
          idx_t oid = oids[o];
          oids[o] = oid + offset;
        }
      }
      iids.emplace_back(std::move(oids));
      break;
    } case PhysicalOperatorType::HASH_GROUP_BY:
      case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
      // 1) getdata()
      D_ASSERT(lsn < tlog->finalize_states_log.size());
      idx_t res_count = tlog->finalize_states_log[lsn].count;
      auto payload = tlog->finalize_states_log[lsn].addresses;
      for (auto& oid : oids) {
        auto addr = payload[oid];
        if (debug)  std::cout << oid << " addr:  "  << (void*)addr << " " << res_count << " "  << lsn << std::endl;

        // 2) combine()
        // look across all threads to see who wrote to payload[oid]
        // multiple threads could write to addr 
        vector<pair<idx_t, data_ptr_t>> partition_addr; 
        for (int i=0; i < thread_vec.size(); i++) {
          void* tkey = thread_vec[i];
          shared_ptr<Log>& tlog = log[tkey];
          if (debug)  std::cout << i << "combine log size  " << tlog->combine_log.size() << std::endl;

          if (tlog->combine_log.size() == 0) {
            partition_addr.push_back({i, addr});
          }
          
          for (int k=tlog->combine_log.size()-1; k >= 0; --k) {
            idx_t res_count = log[tkey]->combine_log[k].count;
            auto src = log[tkey]->combine_log[k].src;
            auto target = log[tkey]->combine_log[k].target;
            bool stop = false;
            for (idx_t j=0; j < res_count; ++j) {
              if (debug)  std::cout << i << " combine:  "  << (void*)addr << " " << (void*)target[j] << " " << (void*)src[j] << std::endl;
              if (addr == target[j]) {
                partition_addr.push_back({i, src[j]}); 
                stop = true;
                break;
              }
            }
            if (stop) break;
          }
        }      
        if (partition_addr.size() == 0) {
          for (int i=0; i < thread_vec.size(); i++) {
            void* tkey = thread_vec[i];
            shared_ptr<Log>& tlog = log[tkey];
            partition_addr.push_back({i, addr});
          }
        }
          

        // 3) sink()
        if (debug) std::cout << "partition_addr : " << partition_addr.size() << std::endl;
        for (auto& p : partition_addr) {
          idx_t thread_id = p.first;
          if (debug) std::cout << thread_id << " scatter  :  "  << (void*)p.second << std::endl;
          for (int i=0; i < thread_vec.size(); i++) {
            void* tkey = thread_vec[i];
            shared_ptr<Log>& tlog = log[tkey];
            // TODO: build index key (addr -> [(lsn, tid)])
            if (type == PhysicalOperatorType::HASH_GROUP_BY) {
              for (int k=0; k < tlog->scatter_log.size(); ++k) {
                idx_t count = tlog->scatter_log[k].count;
                data_ptr_t* payload = tlog->scatter_log[k].addresses;
                vector<idx_t> temp_iids;
                for (idx_t j=0; j < count; ++j) {
                  if (payload[j] == p.second) temp_iids.emplace_back(j);
                }
                unordered_map<idx_t, vector<idx_t>> oids_per_lsn;
                if (!temp_iids.empty()) {
                  idx_t in_pk = k * (thread_vec.size() + 1) + i;
                  oids_per_lsn[in_pk] = std::move(temp_iids);
                  GetNextChild(children[0])->LQ( oids_per_lsn, iids );
                }
              }
            } else {
              for (int k=0; k < tlog->int_scatter_log.size(); ++k) {
                idx_t count = tlog->int_scatter_log[k].count;
                int* payload = tlog->int_scatter_log[k].addresses;
                int tuple_size = tlog->tuple_size;
                uintptr_t fixed = tlog->fixed;
                unordered_map<idx_t, vector<idx_t>> oids_per_lsn;
                vector<idx_t> temp_iids;
                for (idx_t j=0; j < count; ++j) {
                  data_ptr_t key = (data_ptr_t)(fixed + payload[j] * tuple_size);
                  if (key == p.second) temp_iids.push_back(j);
                }
                if (!temp_iids.empty()) {
                  idx_t in_pk = k * (thread_vec.size() + 1) + i;
                  oids_per_lsn[in_pk] = std::move(temp_iids);
                  GetNextChild(children[0])->LQ( oids_per_lsn, iids );
                }
              }
            }
          }
        }
      }
      break;
    } case PhysicalOperatorType::HASH_JOIN: {
      // 'source' : out
      D_ASSERT(lsn < tlog->execute_internal.size());
      int blsn = tlog->execute_internal[lsn].first-1;
      if (!tlog->perfect_probe_ht_log.empty()) {
        idx_t count = tlog->perfect_probe_ht_log[blsn].count;
        auto left = tlog->perfect_probe_ht_log[blsn].left;
        auto right = tlog->perfect_probe_ht_log[blsn].right;
        if (left != nullptr) {
          for (idx_t o=0; o < oids.size(); ++o) {
            idx_t oid = oids[o];
            oids[o] = left[oid];
          }
          // right:  track right[oid]
        }
      } else if (!tlog->join_gather_log.empty()) {
        idx_t count = tlog->join_gather_log[blsn].count;
        auto payload = tlog->join_gather_log[blsn].rhs;
        auto lhs = tlog->join_gather_log[blsn].lhs;
        if (lhs) {
          for (idx_t o=0; o < oids.size(); ++o) {
            idx_t oid = oids[o];
            oids[o] = lhs[oid];
          }
        }
        // right:  track payload[oid]
      }
      idx_t in_lsn = blsn; // TODO: get child_lsn
      unordered_map<idx_t, vector<idx_t>> oids_per_lsn;
      idx_t in_pk = in_lsn * (max_threads + 1) + thread_idx;
      oids_per_lsn[in_pk] = std::move(oids);
      GetNextChild(children[0])->LQ( oids_per_lsn, iids );
      break;
      // return  LQ(log_context);
  } case PhysicalOperatorType::STREAMING_LIMIT: {
  } case PhysicalOperatorType::LIMIT: {
  /*
    if (data_idx >= log->limit_offset.size()) return {};
    idx_t start = log->limit_offset[data_idx].start;
    idx_t count = log->limit_offset[data_idx].end;
    idx_t offset = log->limit_offset[data_idx].in_start;
    output[0].push_back(start+offset+local_oid);
    break;
  } case PhysicalOperatorType::PIECEWISE_MERGE_JOIN: {
  } case PhysicalOperatorType::NESTED_LOOP_JOIN: {
    if (data_idx >= log->nlj_log.size()) return {};
    int lsn = log->execute_internal[data_idx].first-1;
    idx_t count = log->nlj_log[lsn].count;
    if (log->nlj_log[lsn].left) {
      sel_t* left_ptr = log->nlj_log[lsn].left.get();
      output[0].push_back(left_ptr[local_oid]);
    } else {
      output[0].push_back(0);
    }
    
    if (log->nlj_log[lsn].right) {
      sel_t* right_ptr = log->nlj_log[lsn].right.get();
      output[1].push_back(right_ptr[local_oid]);
    } else {
      output[1].push_back(0);
    }
    break;
  } case PhysicalOperatorType::CROSS_PRODUCT: {
    if (data_idx >= log->cross_log.size()) return {};
    int lsn = log->execute_internal[data_idx].first-1;

    idx_t branch_scan_lhs = log->cross_log[lsn].branch_scan_lhs;
    idx_t count = log->cross_log[lsn].count;
    idx_t offset = log->cross_log[lsn].in_start;
    idx_t position_in_chunk = log->cross_log[lsn].position_in_chunk;
    idx_t scan_position = log->cross_log[lsn].scan_position;
    
    if (branch_scan_lhs == false) {
      output[0].push_back(local_oid+offset);
      output[1].push_back(scan_position + position_in_chunk);
    } else {
      output[0].push_back(offset + position_in_chunk);
      output[1].push_back(local_oid+scan_position);
    }
    break;
    */
    } default: {}	}
    

  }

}

vector<idx_t> OperatorLineage::ResolveGlobal(idx_t oid) {
  bool debug = true;
  if (debug)
    std::cout << "ResolveGlobal: " << oid << " " << thread_vec.size() << std::endl;
  // iterate over all partitions
  // find the one oid IN its range. return: thread_idx, lsn, local_oid
  vector<idx_t> log_context;
  idx_t max_threads = thread_vec.size();
	switch (type) {
	case PhysicalOperatorType::FILTER: {
    for (idx_t i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      shared_ptr<Log>& tlog = log[tkey];
      for (idx_t lsn=0; lsn  < tlog->execute_internal.size(); ++lsn) {
        int blsn = tlog->execute_internal[lsn].first-1;
        int branch = tlog->execute_internal[lsn].second;
        idx_t count = 0, offset = 0;
        if (branch == 0) {
          count = tlog->filter_log[blsn].count;
          offset = tlog->filter_log[blsn].in_start;
        } else {
          count = branch;
          offset = tlog->all_filter_log[blsn];
        }
        if (oid < offset + count && oid >= offset) {
          idx_t pk = lsn * (max_threads + 1) + i;
          return {pk, oid-offset};
        }
      }
    }      
    break;
  } case PhysicalOperatorType::ORDER_BY: {
    for (idx_t i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      shared_ptr<Log>& tlog = log[tkey];
      if (debug)  std::cout << "ORDER BY i(" << i << ") " << tlog->reorder_log.size() << std::endl;
      for (idx_t lsn=0; lsn  < tlog->reorder_log.size(); ++lsn) {
          if (debug)  std::cout << "size: " << tlog->reorder_log[lsn].size() << std::endl;
          if (oid >= 0 && oid < tlog->reorder_log[lsn].size()) {// e: vector<idx_t>
            idx_t pk = lsn * (max_threads + 1) + i;
            return {pk, oid};
          }
      }
    }      
    break;
  } case PhysicalOperatorType::TABLE_SCAN: {
    for (idx_t i=0; i < thread_vec.size(); i++) {
      idx_t offset = 0;
      void* tkey = thread_vec[i];
      shared_ptr<Log>& tlog = log[tkey];
      for (idx_t lsn=0; lsn  < tlog->row_group_log.size(); ++lsn) {
        int count = tlog->row_group_log[lsn].count;
        if (oid < offset + count && oid >= offset) {
          idx_t pk = lsn * (max_threads + 1) + i;
          return {pk, oid-offset};
        }
        offset += count;
      }
    }      
    break;
  } case PhysicalOperatorType::HASH_GROUP_BY:
    case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
    // 1) getdata()
    for (idx_t i=0; i < thread_vec.size(); i++) {
      idx_t offset = 0;
      void* tkey = thread_vec[i];
      shared_ptr<Log>& tlog = log[tkey];
      if (debug)
        std::cout << "GROUP BY i (" << i << ") " << tlog->finalize_states_log.size() << std::endl;
      for (idx_t lsn=0; lsn  < tlog->finalize_states_log.size(); ++lsn) {
        idx_t count = tlog->finalize_states_log[lsn].count;
        if (debug)
          std::cout << lsn << " -> oid(" << oid << ") offset(" << offset << ") count:" << count << std::endl;
        if (oid >= offset && oid < offset + count) {
          idx_t pk = lsn * (max_threads + 1) + i;
          return {pk, oid - offset};
        }
      }
    }
    break;
  } case PhysicalOperatorType::HASH_JOIN: {
    // check across probe find the partition oid belong to
    for (idx_t i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      shared_ptr<Log>& tlog = log[tkey];
      idx_t offset = 0;
      for (idx_t lsn=0; lsn  < tlog->execute_internal.size(); ++lsn) {
        int blsn = tlog->execute_internal[lsn].first-1;
        idx_t count = 0;
        if (!tlog->perfect_probe_ht_log.empty()) {
          count = tlog->perfect_probe_ht_log[blsn].count;
        } else if (!tlog->join_gather_log.empty()) {
          count = tlog->join_gather_log[blsn].count;
        }
        if (oid >= offset && oid < offset + count) {
          idx_t pk = lsn * (max_threads + 1) + i;
          return {pk, oid-offset};
        }
      }
    }
		break;
  } default: {}
	}
  
  return {};
}


} // namespace duckdb
#endif
