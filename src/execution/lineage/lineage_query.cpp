#ifdef LINEAGE

#include "duckdb/execution/lineage/operator_lineage.hpp"

namespace duckdb {

shared_ptr<OperatorLineage> GetNextChild(shared_ptr<OperatorLineage>& lop) {
  if (!lop || lop->children.size() == 0) return lop;
  auto& child  = lop;
  while (child->type == PhysicalOperatorType::PROJECTION) {
    child = child->children[0];
  }
  return child;
}

idx_t encode(idx_t lsn, idx_t tid, idx_t max_threads) {
    return lsn * max_threads + tid;
}

pair<idx_t, idx_t> decode_pk(idx_t pk, idx_t max_threads) {
  return { pk % (max_threads) , pk / (max_threads) };
}

unordered_map<void*, idx_t> get_threads_map(std::vector<void*>& thread_vec) {
  unordered_map<void*, idx_t> map_local;
  for (idx_t ctid=0; ctid < thread_vec.size(); ctid++) {
    map_local[ thread_vec[ctid] ] = ctid;
  }
  return std::move(map_local);
}

// fill out_log_context
void OperatorLineage::get_right_match(vector<data_ptr_t>& in_key,
    unordered_map<idx_t, vector<idx_t>>& oids_per_lsn) {
  auto rchild = GetNextChild(children[1]);
  idx_t rchild_max_threads = rchild->thread_vec.size();
  unordered_map<void*, idx_t> thread_vec_map_local = get_threads_map(rchild->thread_vec);
  idx_t max_threads = thread_vec.size();
  // TODO: build incremental index
  if (scatter_sel_log_index.empty() /* && incremental */) {
    for (int i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      auto& tlog = log[tkey];
      if (tlog->scatter_sel_log.empty()) continue;
      auto tid_it = thread_vec_map_local.find(tkey);
      if (tid_it == thread_vec_map_local.end()) continue;
      idx_t rchild_tid = tid_it->second;
      for (int k = 0; k < tlog->scatter_sel_log.size(); k++) {
        idx_t res_count = tlog->scatter_sel_log[k].count;
        auto payload = tlog->scatter_sel_log[k].addresses;
        auto sel = tlog->scatter_sel_log[k].sel;
        idx_t in_pk = encode(k, rchild_tid, rchild_max_threads);
        idx_t pk = k * thread_vec.size() + i;
        //std::cout << "Lookup: " << k << " " << in_pk << " in_keys " << in_key.size() << std::endl;
        if (sel) {
          for (idx_t o=0; o < in_key.size(); ++o)
            for (idx_t j=0; j < res_count; ++j)
             // if (in_key[o] == payload[j]) oids_per_lsn[in_pk].emplace_back(sel[j]);
              scatter_sel_log_index[payload[j]] = {pk, sel[j]}; // incremental
        } else {
          for (idx_t o=0; o < in_key.size(); ++o)
            for (idx_t j=0; j < res_count; ++j)
              // if (in_key[o] == payload[j])   oids_per_lsn[in_pk].emplace_back(j);
              scatter_sel_log_index[payload[j]] = {pk, j}; // incemental
        }
          
      }
    }
  }
  
  unordered_map<void*, bool> seen;
  if (!scatter_sel_log_index.empty()) {
      for (idx_t o=0; o < in_key.size(); ++o) {
        if (seen.find( in_key[o] ) == seen.end()) {
          seen[in_key[o]] = true;
        } else {
          continue;
        }
        auto res = scatter_sel_log_index.find( in_key[o] );
        if (res == scatter_sel_log_index.end()) continue;
        std::pair<idx_t, idx_t>& pk_iid = res->second;
        std::pair<idx_t, idx_t> tid_lsn = decode_pk(pk_iid.first, max_threads);
        idx_t thread_idx = tid_lsn.first;
        void* tkey = thread_vec[thread_idx];
        idx_t lsn = tid_lsn.second;
        idx_t iid = pk_iid.second;
        // std::cout << o << " " << max_threads << " " << (void*)in_key[o] << " " << pk_iid.first << " " << pk_iid.second << " " << tid_lsn.first << " " << tid_lsn.second <<std::endl;
        auto tid_it = thread_vec_map_local.find(tkey);
        if (tid_it == thread_vec_map_local.end()) continue;
        idx_t rchild_tid = tid_it->second;
        idx_t in_pk = encode(lsn, rchild_tid, rchild_max_threads);
        oids_per_lsn[in_pk].emplace_back(iid);
      }
      return;
  }
}

void OperatorLineage::get_perfect_right_match(vector<sel_t>& in_key,
    vector<data_ptr_t>& scatter_keys) {
  if (perfect_full_scan_ht_index.empty() /* && incremental */) {
    for (int i=0; i < thread_vec.size(); i++) { 
      void* tkey = thread_vec[i];
      auto& tlog = log[tkey];
      if (tlog->perfect_full_scan_ht_log.empty()) continue;
      // TODO: build incremental index
      for (int k = 0; k < tlog->perfect_full_scan_ht_log.size(); k++) {
        idx_t key_count = tlog->perfect_full_scan_ht_log[k].key_count;
        idx_t ht_count = tlog->perfect_full_scan_ht_log[k].ht_count;
        // if (debug) std::cout << "k(" << k << ") , key_count(" << key_count << "), ht_count( " << ht_count << ")" << std::endl;
        for (idx_t o=0; o < in_key.size(); ++o) {
          // check if in_keys[o] is in perfect_full_scan_ht_index, if not then find it and add it
          for (int e=0; e < key_count; e++) {
            sel_t build_idx = tlog->perfect_full_scan_ht_log[k].sel_build->owned_data.get()[e];
            sel_t tuples_idx = tlog->perfect_full_scan_ht_log[k].sel_tuples->owned_data.get()[e];
            data_ptr_t* ptr = (data_ptr_t*)tlog->perfect_full_scan_ht_log[k].row_locations->GetData();
            perfect_full_scan_ht_index[build_idx] = ptr[tuples_idx];
          }
        }
      }
    }
  }
  
  for (idx_t o=0; o < in_key.size(); ++o) {
    auto res = perfect_full_scan_ht_index.find(in_key[o]);
    if (res == perfect_full_scan_ht_index.end()) continue;
    scatter_keys[o] = res->second;
  }
}

// 1:1 mapping update in place
idx_t physical_filter_in_place_update(Log* tlog, idx_t lsn, vector<idx_t>& oids) {
  //std::cout << lsn << " " << tlog->execute_internal.size() << std::endl;
  D_ASSERT(lsn < tlog->execute_internal.size());
  int blsn = tlog->execute_internal[lsn].first-1;
  int in_lsn = tlog->execute_internal[lsn].second-1;
  auto ptr = tlog->filter_log[blsn].sel;
  auto count = tlog->filter_log[blsn].count;
  if (ptr) {
    for (idx_t o=0; o < oids.size(); ++o) {
      idx_t oid = oids[o];
      oids[o] = ptr[oid];
    }
  } else {
    for (idx_t o=0; o < oids.size(); ++o) {
      idx_t oid = oids[o];
      oids[o] = oid;
    }
  }

  return in_lsn;
}

void fill_partition_addr(vector<pair<idx_t, data_ptr_t>>& partition_addr, std::vector<void*>& thread_vec,
  unordered_map<void*, shared_ptr<Log>>& log, data_ptr_t addr, bool use_combine) {
  
  if (partition_addr.size() == 0 && use_combine) {
    for (int i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      shared_ptr<Log>& tlog = log[tkey];
      for (int k=tlog->combine_log.size()-1; k >= 0; --k) {
        idx_t res_count = tlog->combine_log[k].count;
        auto src = tlog->combine_log[k].src;
        auto target = tlog->combine_log[k].target;
        for (idx_t j=0; j < res_count; ++j) {
          if (target[j] == addr) {
            partition_addr.emplace_back(i, src[j]); 
            break;
          }
        }
      }
    }
  }

  if (partition_addr.size() == 0) {
    for (int i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      shared_ptr<Log>& ttlog = log[tkey];
      partition_addr.push_back({i, addr});
    }
  }
}

void OperatorLineage::LQ(unordered_map<idx_t, vector<idx_t>>& log_context,
                                unordered_map<idx_t, vector<vector<idx_t>>>& sources, bool all) {
    bool debug = false;
    idx_t max_threads = thread_vec.size();
    if (debug) std::cout << "LQ() -> " << operator_id << " " << name << " |log_context|:" << log_context.size() <<  " |sources|: " << sources.size() << 
      ", |thread_vec|: " << max_threads << std::endl;
    switch (type) {
    case PhysicalOperatorType::DELIM_SCAN:
    case PhysicalOperatorType::RIGHT_DELIM_JOIN:
    case PhysicalOperatorType::LEFT_DELIM_JOIN: {
      D_ASSERT(children.size() == 1);
      children[0]->LQ(log_context, sources);
      break;
    } case PhysicalOperatorType::UNGROUPED_AGGREGATE: {
      auto child = GetNextChild(children[0]);
      log_context.clear();
      child->LQ( log_context, sources , true);
      break;
    } case PhysicalOperatorType::FILTER: {
      unordered_map<idx_t, vector<idx_t>> oids_per_lsn;
      auto child = GetNextChild(children[0]);
      idx_t child_max_threads = child->thread_vec.size();
      unordered_map<void*, idx_t> thread_vec_map_local = get_threads_map(child->thread_vec);
      for (auto& e : log_context) {
        pair<idx_t, idx_t> tid_lsn = decode_pk(e.first, max_threads);
        vector<idx_t>& oids = e.second;
        void* thread_val  = thread_vec[tid_lsn.first];
        auto& tlog = log[thread_val];
        auto tid_it = thread_vec_map_local.find(thread_val);
        if (tid_it == thread_vec_map_local.end()) continue;
        idx_t child_tid = tid_it->second;
        idx_t in_lsn = physical_filter_in_place_update(tlog.get(), tid_lsn.second, oids);
        if (debug) std::cout << "FILTER: tid(" <<  tid_lsn.first << "), lsn(" << tid_lsn.second << ")," << " |oids|: " << oids.size()
          << " ctid(" << child_tid <<  "  in_lsn(" << in_lsn << ")" << std::endl;
        idx_t in_pk = encode(in_lsn, child_tid, child_max_threads);
        oids_per_lsn[in_pk] = std::move(oids); 
      }

      if (all) {
          for (idx_t i=0; i < thread_vec.size(); i++) {
            void* tkey = thread_vec[i];
            shared_ptr<Log>& tlog = log[tkey];
            auto it = std::find(child->thread_vec.begin(), child->thread_vec.end(), tkey); // TODO: replace with index
            if (it == child->thread_vec.end()) continue;
            idx_t child_tid = std::distance(child->thread_vec.begin(), it);

            for (idx_t lsn=0; lsn  < tlog->execute_internal.size(); ++lsn) {
              int blsn = tlog->execute_internal[lsn].first-1;
              int in_lsn = tlog->execute_internal[lsn].second-1;
              auto ptr = tlog->filter_log[blsn].sel;
              idx_t count = tlog->filter_log[blsn].count;
              vector<idx_t> oids(count);
              idx_t in_pk = encode(in_lsn, child_tid, child_max_threads);
              if (ptr) {
                for (idx_t o=0; o < count; ++o) {
                  oids[o] = ptr[o];
                }
              } else {
                for (idx_t o=0; o < count; ++o) {
                  oids[o] = o;
                }
              }
              oids_per_lsn[in_pk] = std::move(oids); 
            }
          }      
      }

      child->LQ( oids_per_lsn, sources );
      break;
    } case PhysicalOperatorType::ORDER_BY: {
      unordered_map<idx_t, vector<idx_t>> oids_per_lsn;
      auto child = GetNextChild(children[0]);
      idx_t child_max_threads = child->thread_vec.size();
      unordered_map<void*, idx_t> thread_vec_map_local = get_threads_map(child->thread_vec);
      for (auto& e : log_context) {
        pair<idx_t, idx_t> tid_lsn = decode_pk(e.first, max_threads);
        vector<idx_t>& oids = e.second;
        idx_t lsn = tid_lsn.second;
        void* thread_val  = thread_vec[tid_lsn.first];
        auto& tlog = log[thread_val];
        if (debug) std::cout << "ORDER BY: tid(" <<  tid_lsn.first << "), lsn(" << lsn << ")," << " |oids|: " << oids.size() << std::endl;
        D_ASSERT(tid_lsn.second < tlog->reorder_log.size());
        idx_t child_tid = 0;

          // TODO: replace with index
          for (int i=0; i < thread_vec.size(); i++) {
            void* ttkey = thread_vec[i];
            shared_ptr<Log>& ttlog = log[ttkey];
            auto tid_it = thread_vec_map_local.find(ttkey);
            if (tid_it == thread_vec_map_local.end()) continue;
            idx_t child_tid = tid_it->second;
            
            // TODO: replace with a zone map index
            idx_t offset = 0;
            for (idx_t blsn=0; blsn  < ttlog->single_int_log.size(); ++blsn) {
              idx_t pk = encode(blsn, child_tid, child_max_threads);
              idx_t count = ttlog->single_int_log[blsn];
              for (idx_t o=0; o < oids.size(); ++o) {
                idx_t oid = oids[o];
                idx_t sink_oid = tlog->reorder_log[lsn][oid];
                if (sink_oid >= offset && sink_oid < offset + count) {
                  oids_per_lsn[pk].push_back(sink_oid - offset);
                }
              }
              offset += count;
            }
          }
      }
      child->LQ( oids_per_lsn, sources );
      break;
    } case PhysicalOperatorType::COLUMN_DATA_SCAN: {
      // todo: get partition offsets
      for (auto& e : log_context) {
        pair<idx_t, idx_t> tid_lsn = decode_pk(e.first, max_threads);
        vector<idx_t>& oids = e.second;
        sources[operator_id].emplace_back(std::move(oids));
      }
      break;
    } case PhysicalOperatorType::TABLE_SCAN: { // 1:1 in place update
      for (auto& e : log_context) {
        pair<idx_t, idx_t> tid_lsn = decode_pk(e.first, max_threads);
        idx_t lsn = tid_lsn.second;
        vector<idx_t>& oids = e.second;
        void* thread_val  = thread_vec[tid_lsn.first];
        auto& tlog = log[thread_val];
        if (debug) std::cout << "TABLE SCAN: tid(" <<  tid_lsn.first << "), lsn(" << tid_lsn.second << ")," << " |oids|: " << oids.size() << std::endl;
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
        sources[operator_id].emplace_back(std::move(oids));
      }
      
      if (all) {
        std::cout << "all " << std::endl;
          for (idx_t i=0; i < thread_vec.size(); i++) {
            void* tkey = thread_vec[i];
            shared_ptr<Log>& tlog = log[tkey];
            for (idx_t lsn=0; lsn  < tlog->row_group_log.size(); ++lsn) {
              int count = tlog->row_group_log[lsn].count;
              idx_t offset = tlog->row_group_log[lsn].start + tlog->row_group_log[lsn].vector_index;
              vector<idx_t> oids(count);
              if (tlog->row_group_log[lsn].sel) {
                for (idx_t o=0; o < count; ++o) {
                  oids[o] = tlog->row_group_log[lsn].sel[o] + offset;
                }
              } else {
                for (idx_t o=0; o < count; ++o) {
                  oids[o] = o + offset;
                }
              }
              sources[operator_id].emplace_back(std::move(oids));
            }
          }      
      }

      break;
    } case PhysicalOperatorType::HASH_GROUP_BY:
      case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
      auto child = GetNextChild(children[0]);
      idx_t child_max_threads = child->thread_vec.size();
      unordered_map<void*, idx_t> thread_vec_map_local = get_threads_map(child->thread_vec);
      unordered_map<idx_t, vector<idx_t>> oids_per_lsn;
      for (auto& e : log_context) {
        pair<idx_t, idx_t> tid_lsn = decode_pk(e.first, max_threads);
        vector<idx_t>& oids = e.second;
        void* thread_val  = thread_vec[tid_lsn.first];
        auto& tlog = log[thread_val];
        // 1) getdata()
        D_ASSERT(tid_lsn.second < tlog->finalize_states_log.size());
        idx_t res_count = tlog->finalize_states_log[tid_lsn.second].count;
        auto payload = tlog->finalize_states_log[tid_lsn.second].addresses;
        if (debug) std::cout << "AGGS: tid(" <<  tid_lsn.first << "), lsn(" << tid_lsn.second << "),"
          << " |oids|: " << oids.size() << " " << res_count <<  " |partition_addr_index|: " << partition_addr_index.size()
          << " |thread_vec_map_local| " <<  thread_vec_map_local.size() <<
          " |finalize states log| " << tlog->finalize_states_log.size() << std::endl;
        for (auto& oid : oids) { // 6
            auto addr = payload[oid];
            auto sink_it = partition_addr_index.find(addr);
            if (sink_it == partition_addr_index.end() && !scatter_log_index_full.empty()) {
                auto it_index = scatter_log_index_full.find(addr);
                if (it_index == scatter_log_index_full.end()) continue;
                for (auto& elem : it_index->second) {
                  if (debug) std::cout << "AGGS: in_pk: "<< elem.first << " " << elem.second.size() << std::endl;
                  oids_per_lsn[elem.first].insert(oids_per_lsn[elem.first].end(), elem.second.begin(), elem.second.end());
                }
                continue;
            } else if (sink_it == partition_addr_index.end()) {
              bool use_combine = scatter_log_index_full.empty();
              fill_partition_addr(partition_addr_index[addr], thread_vec, log, addr, use_combine);
              sink_it = partition_addr_index.find(addr);
            }

            if (debug)  std::cout << "GetData()  " << oid << " ->  "<<  (void*)addr << std::endl;
            for (auto& p : sink_it->second) { // 5 for each local partition
                idx_t thread_id = p.first;
                void* tkey = thread_vec[thread_id];
                shared_ptr<Log>& ttlog = log[tkey];
                auto tid_it = thread_vec_map_local.find(tkey);
                if (tid_it == thread_vec_map_local.end()) continue;
                idx_t child_tid = tid_it->second;
                if (debug) std::cout << "tid: " << thread_id << " Scatter:  "  << (void*)p.second
                    << " |ttlog->scatter_log|: " << ttlog->scatter_log.size()  << " |ttlog->int_scatter_log|: " << ttlog->int_scatter_log.size()
                    << " |partition_addr| : " << sink_it->second.size() << std::endl;
                auto it_index = scatter_log_index_full.find(p.second);
                if (it_index != scatter_log_index_full.end()) {
                    for (auto& elem : it_index->second) {
                      if (debug) std::cout << "AGGS: in_pk: "<< elem.first << " " << elem.second.size() << std::endl;
                      oids_per_lsn[elem.first].insert(oids_per_lsn[elem.first].end(), elem.second.begin(), elem.second.end());
                    }
                    continue;
                }
                  
                if (type == PhysicalOperatorType::HASH_GROUP_BY) {
                    if (ttlog->scatter_log_inverse.empty()) {
                      for (int k=0; k < ttlog->scatter_log.size(); ++k) {
                        idx_t in_pk = encode(k, child_tid, child_max_threads);
                        idx_t count = ttlog->scatter_log[k].count;
                        data_ptr_t* payload = ttlog->scatter_log[k].addresses;
                        if (log_context.size() > 1 || oids.size() > 1) {
                          for (idx_t j=0; j < count; ++j) {
                            ttlog->scatter_log_inverse[payload[j]].insert(k); // useless. ues scatter_log_index_full
                            if (payload[j] == p.second) oids_per_lsn[in_pk].emplace_back(j);
                          }
                        } else {
                          for (idx_t j=0; j < count; ++j) {
                            if (payload[j] == p.second) oids_per_lsn[in_pk].emplace_back(j);
                          }
                        }
                      }
                    } else {
                      for (auto k : ttlog->scatter_log_inverse[p.second]) {
                        idx_t in_pk = encode(k, child_tid, child_max_threads);
                        idx_t count = ttlog->scatter_log[k].count;
                        data_ptr_t* payload = ttlog->scatter_log[k].addresses;
                        for (idx_t j=0; j < count; ++j) {
                          if (payload[j] == p.second) oids_per_lsn[in_pk].emplace_back(j);
                        }
                      }
                    }
                } else { // 4
                    if (ttlog->scatter_log_inverse.empty()) {
                      for (int k=0; k < ttlog->int_scatter_log.size(); ++k) { // 3
                        int tuple_size = ttlog->tuple_size;
                        uintptr_t fixed = ttlog->fixed;
                        idx_t in_pk = encode(k, child_tid, child_max_threads);
                        int* payload = ttlog->int_scatter_log[k].addresses;
                        idx_t count = ttlog->int_scatter_log[k].count;
                        if (log_context.size() > 1 || oids.size() > 1) {
                          for (idx_t j=0; j < count; ++j) {
                            data_ptr_t key = (data_ptr_t)(fixed + payload[j] * tuple_size);
                            ttlog->scatter_log_inverse[key].insert(k); // if incremental and parent = agg
                            if (key == p.second) oids_per_lsn[in_pk].emplace_back(j);
                          }
                        } else {
                          for (idx_t j=0; j < count; ++j) {
                            data_ptr_t key = (data_ptr_t)(fixed + payload[j] * tuple_size);
                            if (key == p.second) oids_per_lsn[in_pk].emplace_back(j);
                          }
                        }
                      } // 3
                    } else {
                      for (auto k : ttlog->scatter_log_inverse[p.second]) {
                        int tuple_size = ttlog->tuple_size;
                        uintptr_t fixed = ttlog->fixed;
                        idx_t in_pk = encode(k, child_tid, child_max_threads);
                        idx_t count = ttlog->int_scatter_log[k].count;
                        int* payload = ttlog->int_scatter_log[k].addresses;
                        for (idx_t j=0; j < count; ++j) {
                            data_ptr_t key = (data_ptr_t)(fixed + payload[j] * tuple_size);
                          if (key == p.second) oids_per_lsn[in_pk].emplace_back(j);
                        }
                      }
                    }
              } // 4
            } // 5
          } // 6
      }
      if (debug) std::cout << "oids_per_lsn: " << oids_per_lsn.size() << std::endl;
      // if child is agg, then include all
      child->LQ( oids_per_lsn, sources );
      break;
    } case PhysicalOperatorType::HASH_JOIN: {
      unordered_map<idx_t, vector<idx_t>> oids_per_lsn;
      auto child = GetNextChild(children[0]);
      idx_t child_max_threads = child->thread_vec.size();
      unordered_map<idx_t, vector<idx_t>> right_oids_per_lsn;
      unordered_map<void*, idx_t> thread_vec_map_local = get_threads_map(child->thread_vec);

      for (auto& e : log_context) {
        pair<idx_t, idx_t> tid_lsn = decode_pk(e.first, max_threads);
        vector<idx_t>& oids = e.second;
        idx_t lsn = tid_lsn.second;
        void* thread_val  = thread_vec[tid_lsn.first];
        auto& tlog = log[thread_val];
        if (debug) std::cout << "JOIN: tid(" <<  tid_lsn.first << "), lsn(" << tid_lsn.second << ")," << " |oids|: " << oids.size() << std::endl;
        
        auto tid_it = thread_vec_map_local.find(thread_val);
        if (tid_it == thread_vec_map_local.end()) continue;
        idx_t child_tid = tid_it->second;

        D_ASSERT(lsn < tlog->execute_internal.size());
        int blsn = tlog->execute_internal[lsn].first-1;
        int in_lsn = tlog->execute_internal[lsn].second-1;
              
        if (!tlog->perfect_probe_ht_log.empty()) {
          idx_t count = tlog->perfect_probe_ht_log[blsn].count;
          auto left = tlog->perfect_probe_ht_log[blsn].left;
          auto right = tlog->perfect_probe_ht_log[blsn].right;
          vector<sel_t> in_right(oids.size());
          if (right) {
            for (idx_t o=0; o < oids.size(); ++o) {
              idx_t oid = oids[o];
              in_right[o] = right[oid];
            }
            // std::cout << "get right 1" << std::endl;
            vector<data_ptr_t> scatter_keys(in_right.size());
            get_perfect_right_match(in_right, scatter_keys);
            get_right_match(scatter_keys, right_oids_per_lsn);
          }
          if (left) {
            for (idx_t o=0; o < oids.size(); ++o) {
              idx_t oid = oids[o];
              oids[o] = left[oid];
            }
            idx_t in_pk = encode(in_lsn, child_tid, child_max_threads);
            oids_per_lsn[in_pk] = std::move(oids);
          }
        } else if (!tlog->join_gather_log.empty()) {
          idx_t count = tlog->join_gather_log[blsn].count;
          auto payload = tlog->join_gather_log[blsn].rhs;
          auto lhs = tlog->join_gather_log[blsn].lhs;
          vector<data_ptr_t> in_right(oids.size());
          if (payload) {
            for (idx_t o=0; o < oids.size(); ++o) {
              idx_t oid = oids[o];
              in_right[o] = payload[oid];
            }
            // std::cout << "get right 2" << std::endl;
            get_right_match(in_right, right_oids_per_lsn);
          }
          if (lhs) {
            for (idx_t o=0; o < oids.size(); ++o) {
              idx_t oid = oids[o];
              oids[o] = lhs[oid];
            }
            idx_t in_pk = encode(in_lsn, child_tid, child_max_threads);
            oids_per_lsn[in_pk] = std::move(oids);
          }
        }
                
      }
      
      if (all) { // 4
          for (idx_t i=0; i < thread_vec.size(); i++) { //3
            idx_t offset = 0;
            void* tkey = thread_vec[i];
            shared_ptr<Log>& tlog = log[tkey];
            for (int lsn = 0; lsn < tlog->execute_internal.size(); lsn++) { // 2
              int blsn = tlog->execute_internal[lsn].first-1;
              int in_lsn = tlog->execute_internal[lsn].second-1;
              idx_t in_pk = encode(lsn, i, max_threads);
              
              if (!tlog->perfect_probe_ht_log.empty()) {
                idx_t count = tlog->perfect_probe_ht_log[blsn].count;
                vector<idx_t> oids(count);
                for (idx_t o=0; o < oids.size(); ++o) {
                  oids[o] = o;
                }
                oids_per_lsn[in_pk] = std::move(oids);
              } else if (!tlog->join_gather_log.empty()) { // 1
                idx_t count = tlog->join_gather_log[blsn].count;
                vector<idx_t> oids(count);
                for (idx_t o=0; o < oids.size(); ++o) {
                  oids[o] = o;
                }
                oids_per_lsn[in_pk] = std::move(oids);
              } // 1
            } // 2
          } //3
        LQ( oids_per_lsn, sources );
      } else { // 4
        child->LQ( oids_per_lsn, sources );
        oids_per_lsn.clear();
        // /std::cout << "right child " << right_oids_per_lsn.size() << std::endl;
        GetNextChild(children[1])->LQ( right_oids_per_lsn, sources );
      }
      break;
  } case PhysicalOperatorType::STREAMING_LIMIT: {
    unordered_map<idx_t, vector<idx_t>> oids_per_lsn;
    auto child = GetNextChild(children[0]);
    idx_t child_max_threads = child->thread_vec.size();
    for (auto& e : log_context) {
      pair<idx_t, idx_t> tid_lsn = decode_pk(e.first, max_threads);
      vector<idx_t>& oids = e.second;
      idx_t lsn = tid_lsn.second;
      void* thread_val  = thread_vec[tid_lsn.first];
      auto& tlog = log[thread_val];
    
      D_ASSERT(tid_lsn.second < tlog->execute_internal.size());
      idx_t start = tlog->limit_offset[lsn].start;
      idx_t count = tlog->limit_offset[lsn].end;
      idx_t offset = tlog->limit_offset[lsn].in_start;
      
      auto it = std::find(child->thread_vec.begin(), child->thread_vec.end(), thread_val); // TODO: replace with index
      if (it == child->thread_vec.end()) continue; // ASSERT
      idx_t child_tid = std::distance(child->thread_vec.begin(), it);
      idx_t in_pk = encode(lsn, child_tid, child_max_threads);
      oids_per_lsn[in_pk] = std::move(e.second);
    }
    child->LQ( oids_per_lsn, sources );
    break;
  } case PhysicalOperatorType::LIMIT: {
    // adjust offset; then call child with global id?
    break;
  } case PhysicalOperatorType::PIECEWISE_MERGE_JOIN: {
  } case PhysicalOperatorType::NESTED_LOOP_JOIN: {
      auto rchild = GetNextChild(children[1]);
      auto child = GetNextChild(children[0]);
      idx_t rchild_max_threads = rchild->thread_vec.size();
      idx_t child_max_threads = child->thread_vec.size();
      unordered_map<idx_t, vector<idx_t>> oids_per_lsn;
      unordered_map<idx_t, vector<idx_t>> right_oids_per_lsn;
      unordered_map<void*, idx_t> thread_vec_map_local = get_threads_map(child->thread_vec);
      for (auto& e : log_context) {
        pair<idx_t, idx_t> tid_lsn = decode_pk(e.first, max_threads);
        idx_t lsn = tid_lsn.second;
        vector<idx_t>& oids = e.second;
        void* thread_val  = thread_vec[tid_lsn.first];
        auto& tlog = log[thread_val];
        
        auto tid_it = thread_vec_map_local.find(thread_val);
        if (tid_it == thread_vec_map_local.end()) continue;
        idx_t child_tid = tid_it->second;

        if (debug) std::cout << "NLJ: tid(" <<  tid_lsn.first << "), lsn(" << tid_lsn.second << ")," << 
          " |oids|: " << oids.size() << std::endl;
        D_ASSERT(lsn < tlog->execute_internal.size());
        int blsn = tlog->execute_internal[lsn].first-1;
        int in_lsn = tlog->execute_internal[lsn].second-1;
        
        idx_t count = tlog->nlj_log[blsn].count;
        // hack: need to use global id, resolve to local id
        if (tlog->nlj_log[blsn].right) {
          vector<idx_t> right_oids(oids.size());
          sel_t* right_ptr = tlog->nlj_log[blsn].right.get();
          for (idx_t o=0; o < oids.size(); ++o) {
            idx_t oid = oids[o];
            right_oids[o] = right_ptr[oid];
           // std::cout << o << " " << oid << " " << right_oids[o] << std::endl; 
          }
          right_oids_per_lsn[0] = std::move(right_oids);
         }
        
        idx_t in_pk = encode(in_lsn, child_tid, child_max_threads);
        if (tlog->nlj_log[blsn].left) {
          sel_t* left_ptr = tlog->nlj_log[blsn].left.get();
          for (idx_t o=0; o < oids.size(); ++o) {
            idx_t oid = oids[o];
            oids[o] = left_ptr[oid];
          }
          oids_per_lsn[in_pk] = std::move(oids);
        } 
      }
      child->LQ( oids_per_lsn, sources );
      oids_per_lsn.clear();
      rchild->LQ( right_oids_per_lsn, sources );
    break;
  } default: {}	}

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
          idx_t pk = encode(lsn, i, max_threads);
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
            idx_t pk = encode(lsn, i, max_threads);
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
          idx_t pk = encode(lsn, i, max_threads);
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
          idx_t pk = encode(lsn, i, max_threads);
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
          idx_t pk = encode(lsn, i, max_threads);
          return {pk, oid-offset};
        }
      }
    }
		break;
  } case PhysicalOperatorType::STREAMING_LIMIT: {
    for (idx_t i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      shared_ptr<Log>& tlog = log[tkey];
      idx_t total = 0;
      if (debug) std::cout << "limit " << tlog->limit_offset.size() << std::endl;
      for (idx_t lsn=0; lsn < tlog->limit_offset.size(); lsn++) {
        idx_t start = tlog->limit_offset[lsn].start;
        idx_t count = tlog->limit_offset[lsn].end;
        idx_t offset = tlog->limit_offset[lsn].in_start;
        if (debug) std::cout << oid << " limit: " << start << " " << count << " "
          << offset << " " << lsn << " " << i << " " << max_threads << std::endl;
        if (oid >= total && oid < total + count) {
          idx_t pk = encode(lsn, i, max_threads);
          // todo: check if pipeline breaker or pipelined to get lsn
          // pipelined, get in_lsn; else run resolve on next child
          return {pk, oid+offset};
        }
        total += count;
      }
    }
    break;
  } case PhysicalOperatorType::LIMIT: {
    for (idx_t i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      shared_ptr<Log>& tlog = log[tkey];
      idx_t total = 0;
      for (idx_t lsn=0; lsn < tlog->limit_offset.size(); lsn++) {
        idx_t start = tlog->limit_offset[lsn].start;
        idx_t count = tlog->limit_offset[lsn].end;
        idx_t offset = tlog->limit_offset[lsn].in_start;
        if (oid >= total && oid < total + count) {
          idx_t pk = encode(lsn, i, max_threads);
          std::cout << oid << " limit: " << start << " " << count << " " << offset << " " << lsn << " " << i << " " << max_threads << std::endl;
          // todo: check if pipeline breaker or pipelined to get lsn
          // pipelined, get in_lsn; else run resolve on next child
          return {pk, oid+offset};
        }
        total += count;
      }
    }
  } default: {}
	}
  
  return {};
}


} // namespace duckdb
#endif
