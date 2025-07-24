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

//  TODO: multi-tables
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
    
    bool debug = false;
    if (debug) std::cout << operator_id << " " << name << " LQ: tid(" << thread_idx << "), lsn(" << lsn << ")," << " |oids|: " << oids.size() << 
      " " << max_threads << std::endl;

    switch (type) {
    case PhysicalOperatorType::UNGROUPED_AGGREGATE: {
      std::cout << "TODO: handle aggs" << std::endl;
      break;
    } case PhysicalOperatorType::FILTER: {
      D_ASSERT(lsn < tlog->execute_internal.size());
      int blsn = tlog->execute_internal[lsn].first-1;
      int in_lsn = tlog->execute_internal[lsn].second-1;
      auto ptr = tlog->filter_log[blsn].sel;
      auto count = tlog->filter_log[blsn].count;
      // 1:1 mapping update in place
      if (ptr) {
        for (idx_t o=0; o < oids.size(); ++o) {
          idx_t oid = oids[o];
          /*if (oid >= count) {
            std::cout << o << " " << oids[o] << " " << oid << " " << count << " " << std::endl;
            continue;
          }*/
          oids[o] = ptr[oid];
        }
      } else {
        for (idx_t o=0; o < oids.size(); ++o) {
          idx_t oid = oids[o];
         /* if (oid >= count) {
            std::cout << o << " " << oids[o] << " " << oid << " " << count << " " << std::endl;
            continue;
          }*/
          oids[o] = oid;
        }
      }
      unordered_map<idx_t, vector<idx_t>> oids_per_lsn;
      auto child = GetNextChild(children[0]);
      idx_t child_max_threads = child->thread_vec.size();
      idx_t child_tid = 0;
      for (idx_t ctid=0; ctid < child_max_threads; ++ctid) {
        if (child->thread_vec[ctid] == thread_val) { child_tid = ctid; }
      }
      if (debug) std::cout << "filter -> ctid(" << child_tid << "), tid(" << thread_idx << ", lsn(" << lsn << " ), blsn(" << blsn << "), in_lsn(" << in_lsn << ")" << std::endl;
      idx_t in_pk = in_lsn * (child_max_threads + 1) + child_tid;
      oids_per_lsn[in_pk] = std::move(oids);
      child->LQ( oids_per_lsn, iids );
      break;
    } case PhysicalOperatorType::ORDER_BY: {
      D_ASSERT(lsn < tlog->reorder_log.size());
      auto child = GetNextChild(children[0]);
      idx_t child_max_threads = child->thread_vec.size();
      idx_t child_tid = 0;
      for (int i=0; i < thread_vec.size(); i++) {
        unordered_map<idx_t, vector<idx_t>> oids_per_lsn;
        void* ttkey = thread_vec[i];
        shared_ptr<Log>& ttlog = log[ttkey];
        if (debug)  std::cout << "single int: " << ttlog->single_int_log.size() << std::endl;
        for (idx_t ctid=0; ctid < child_max_threads; ++ctid) {
          if (child->thread_vec[ctid] == ttkey) { child_tid = ctid; }
        }
        // TODO: replace with an index
        idx_t offset = 0;
        for (idx_t blsn=0; blsn  < ttlog->single_int_log.size(); ++blsn) {
          idx_t pk = blsn * (child_max_threads + 1) + child_tid;
          idx_t count = ttlog->single_int_log[blsn];
          for (idx_t o=0; o < oids.size(); ++o) {
            idx_t oid = oids[o];
            idx_t sink_oid = tlog->reorder_log[lsn][oid];
            if (sink_oid >= offset && sink_oid < offset + count) {
            //  std::cout << sink_oid << " " << oid << " " << count << " add : " << sink_oid - offset  << " " << offset << std::endl;
              oids_per_lsn[pk].push_back(sink_oid - offset);
            }
          }
          offset += count;
        }
        child->LQ( oids_per_lsn, iids );
      }
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
      auto child = GetNextChild(children[0]);
      idx_t child_max_threads = child->thread_vec.size();
      for (auto& oid : oids) {
        auto addr = payload[oid];
        if (debug)  std::cout << oid << " addr:  "  << (void*)addr << " " << res_count << " "  << lsn << std::endl;
        // 2) combine()
        // look across all threads to see who wrote to payload[oid]
        // multiple threads could write to addr 
        vector<pair<idx_t, data_ptr_t>> partition_addr; 
        for (int i=0; i < thread_vec.size(); i++) {
          void* tkey = thread_vec[i];
          shared_ptr<Log>& ttlog = log[tkey];
          if (debug)  std::cout << i << " combine log size  " << ttlog->combine_log.size() << std::endl;

          for (int k=ttlog->combine_log.size()-1; k >= 0; --k) {
            idx_t res_count = ttlog->combine_log[k].count;
            auto src = ttlog->combine_log[k].src;
            auto target = ttlog->combine_log[k].target;
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
            shared_ptr<Log>& ttlog = log[tkey];
            partition_addr.push_back({i, addr});
          }
        }
          
        // 3) sink()
        if (debug) std::cout << "partition_addr : " << partition_addr.size() << std::endl;
        for (auto& p : partition_addr) {
          idx_t thread_id = p.first;
          void* tkey = thread_vec[thread_id];
          // TODO: get the index from child with the same tkey
          shared_ptr<Log>& ttlog = log[tkey];
          if (debug) std::cout << thread_id << " scatter  :  "  << (void*)p.second
            << " " << ttlog->scatter_log.size() << " " << ttlog->int_scatter_log.size() <<  std::endl;
          // TODO: build index key (addr -> [(lsn, tid)])
          if (type == PhysicalOperatorType::HASH_GROUP_BY) {
            for (int k=0; k < ttlog->scatter_log.size(); ++k) {
              idx_t count = ttlog->scatter_log[k].count;
              data_ptr_t* payload = ttlog->scatter_log[k].addresses;
              vector<idx_t> temp_iids;
              for (idx_t j=0; j < count; ++j) {
                if (payload[j] == p.second) temp_iids.emplace_back(j);
              }
              unordered_map<idx_t, vector<idx_t>> oids_per_lsn;
              if (!temp_iids.empty()) {
                idx_t child_tid = 0;
                for (idx_t ctid=0; ctid < child_max_threads; ++ctid) {
                  if (child->thread_vec[ctid] == tkey) { child_tid = ctid; }
                }
                if (debug) std::cout << count << " " << k << " aggs -> citd(" << child_tid << "), tid(" << thread_id << ")" << std::endl;
                idx_t in_pk = k *(child_max_threads + 1) + child_tid;
                oids_per_lsn[in_pk] = std::move(temp_iids);
                child->LQ( oids_per_lsn, iids );
              }
            }
          } else {
            for (int k=0; k < ttlog->int_scatter_log.size(); ++k) {
              idx_t count = ttlog->int_scatter_log[k].count;
              int* payload = ttlog->int_scatter_log[k].addresses;
              int tuple_size = ttlog->tuple_size;
              uintptr_t fixed = ttlog->fixed;
              unordered_map<idx_t, vector<idx_t>> oids_per_lsn;
              vector<idx_t> temp_iids;
              for (idx_t j=0; j < count; ++j) {
                data_ptr_t key = (data_ptr_t)(fixed + payload[j] * tuple_size);
                if (key == p.second) temp_iids.push_back(j);
              }
              if (!temp_iids.empty()) {
                idx_t child_tid = 0;
                for (idx_t ctid=0; ctid < child_max_threads; ++ctid) {
                  if (child->thread_vec[ctid] == tkey) { child_tid = ctid; }
                }
                if (debug) std::cout << "aggs -> citd(" << child_tid << "), tid(" << thread_id << ")" << std::endl;
                idx_t in_pk = k * (child_max_threads + 1) + child_tid;
                oids_per_lsn[in_pk] = std::move(temp_iids);
                child->LQ( oids_per_lsn, iids );
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
      int in_lsn = tlog->execute_internal[lsn].second-1;
      if (!tlog->perfect_probe_ht_log.empty()) {
        idx_t count = tlog->perfect_probe_ht_log[blsn].count;
        auto left = tlog->perfect_probe_ht_log[blsn].left;
        auto right = tlog->perfect_probe_ht_log[blsn].right;
        if (left != nullptr) {
          for (idx_t o=0; o < oids.size(); ++o) {
            idx_t oid = oids[o];
           /* if (oid >= count) {
              std::cout << o << " " << oid << " " << count << " " << std::endl;
              continue;
            }*/
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
          /*  if (oid >= count) {
              std::cout << o << " " << oids[o] << " " << oid << " " << count << " " << std::endl;
              continue;
            }*/
            oids[o] = lhs[oid];
          }
        }
        // right:  track payload[oid]
      }
      // TODO: get the index from child with the same tkey
      unordered_map<idx_t, vector<idx_t>> oids_per_lsn;
      auto child = GetNextChild(children[0]);
      idx_t child_max_threads = child->thread_vec.size();
      idx_t child_tid = 0;
      for (idx_t ctid=0; ctid < child_max_threads; ++ctid) {
        if (child->thread_vec[ctid] == thread_val) { child_tid = ctid; }
      }
      if (debug) std::cout << "hj -> ctid(" << child_tid << "), tid(" << thread_idx << "), cn(" << child_max_threads << "), n(" << max_threads << "), lsn("
        << lsn << "), blsn(" << blsn << "), in_lsn(" << in_lsn << ")" << std::endl;
      idx_t in_pk = in_lsn * (child_max_threads + 1) + child_tid;
      oids_per_lsn[in_pk] = std::move(oids);
      child->LQ( oids_per_lsn, iids );
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
  bool debug = false;
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
        count = tlog->filter_log[blsn].count;
        offset = tlog->filter_log[blsn].in_start;
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
