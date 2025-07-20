#ifdef LINEAGE

#include "duckdb/execution/lineage/operator_lineage.hpp"

namespace duckdb {

shared_ptr<OperatorLineage> GetLop(shared_ptr<OperatorLineage> lop, int opid) {
	if (lop == nullptr) return nullptr;
  if (lop->operator_id == opid) return lop;

  for (auto& c : lop->children) {
    auto ret = GetLop(c, opid);
    if (ret != nullptr) return ret;
	}

  return nullptr;
}

  /*
// add BuildIndexes() 
// zone map for ResultCollector
// {oid} -> {lsn}
void OperatorLineage::BuildIndexes() {
  if (processed) return;
  if (thread_vec.empty()) {
    thread_vec.reserve(log.size());
    for (const auto& pair : log) {
      thread_vec.push_back(pair.first);
    }
  }
	switch (type) {
  case PhysicalOperatorType::ORDER_BY:
	case PhysicalOperatorType::TABLE_SCAN:
	case PhysicalOperatorType::FILTER: 
	case PhysicalOperatorType::COLUMN_DATA_SCAN:
	case PhysicalOperatorType::STREAMING_LIMIT:
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN: {
		break;
	}
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
    // the following steps are across multiple functions;
    // build light weight indexes over the output of each function?
    //
    // (1) start from finalize_states; get address for the oid we are looking for
    // build index[oid] -> k , tkey?
    //auto payload = log[tkey]->finalize_states_log[k].addresses;
    // then payload[oid] -> address
    // (2) use address to get the mapping from combine_log, tkey?
    // (3) use the local_address 
    for (int i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      if (log.count(tkey) == 0 || log[tkey]->finalize_states_log.empty()) continue;
      idx_t count_so_far = 0;
      idx_t res_count = log[tkey]->finalize_states_log[k].count;
      // build zone maps; 
      // zm[tkey][count_so_far+res_count] = k; // tkey is passed from parent
      // log[tkey]->finalize_states_log[k][i-offset]
    }      
    // build for combine log to avoid iterating over the artifacts: index[out_address] -> in_address
    // find scatter_sel_log[k]->payload[i] == out_address; use (i, k, tkey) to access log that generated it
    // it depends on the child, if pipeline breaker, then we need to? else?
    // scatter_sel_log -> for each output id (oid) : index[oid].insert(lsn); // all the unique lsn that includes oid
    // we get lsn, we look for oid , if we find oid
    break;
  } case PhysicalOperatorType::HASH_JOIN: {
    // getdata() -> ?
    // execute() -> zonemap ? it is pipelined
    // sink() -> build index
    for (int i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      idx_t count_so_far = 0;
      if (log.count(tkey) == 0 || log[tkey]->scatter_sel_log.empty()) continue;
      // std::cout << "Scatter Join: " << log[tkey]->scatter_sel_log.size() << std::endl;
      for (int k = 0; k < log[tkey]->scatter_sel_log.size(); k++) {
        idx_t res_count = log[tkey]->scatter_sel_log[k].count;
        idx_t in_start = log[tkey]->scatter_sel_log[k].in_start;
        auto payload = log[tkey]->scatter_sel_log[k].addresses;
        //index[payload[j]] -> {k, tkey};
      }
    }
    for (int i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      if (log.count(tkey) == 0 || log[tkey]->perfect_full_scan_ht_log.empty()) continue;
      for (int k = 0; k < log[tkey]->perfect_full_scan_ht_log.size(); k++) {
        idx_t key_count = log[tkey]->perfect_full_scan_ht_log[k].key_count;
        idx_t ht_count = log[tkey]->perfect_full_scan_ht_log[k].ht_count;
        for (int e=0; e < key_count; e++) {
          idx_t build_idx = log[tkey]->perfect_full_scan_ht_log[k].sel_build->owned_data.get()[e];
          idx_t tuples_idx = log[tkey]->perfect_full_scan_ht_log[k].sel_tuples->owned_data.get()[e];
          data_ptr_t* ptr = (data_ptr_t*)log[tkey]->perfect_full_scan_ht_log[k].row_locations->GetData();
          index[build_idx] -> {k, tkey}
        }
      }
    }
    break;
  }
	default: {
		// Lineage unimplemented! TODO all of these :)
	}
	}
	processed = true;
}*/

void OperatorLineage::PostProcess() {
  if (processed) return;
  if (thread_vec.empty()) {
    thread_vec.reserve(log.size());
    for (const auto& pair : log) {
      thread_vec.push_back(pair.first);
    }
  }
	switch (type) {
	case PhysicalOperatorType::TABLE_SCAN:
	case PhysicalOperatorType::FILTER: 
	case PhysicalOperatorType::COLUMN_DATA_SCAN:
	case PhysicalOperatorType::STREAMING_LIMIT:
	case PhysicalOperatorType::LIMIT: {
    break;
                                    }
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
    // gather
    for (int i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      if (log.count(tkey) == 0 || log[tkey]->finalize_states_log.empty()) continue;
      idx_t count_so_far = 0;
      for (int k=0; k < log[tkey]->finalize_states_log.size(); ++k) {
        idx_t res_count = log[tkey]->finalize_states_log[k].count;
        //std::cout << count_so_far+res_count << " finalize states: " << tkey << " " << log[tkey]->finalize_states_log.size() << std::endl;
        auto payload = log[tkey]->finalize_states_log[k].addresses;
        for (idx_t j=0; j < res_count; ++j) {
          if (log_index->codes.find(payload[j]) == log_index->codes.end()) {
            log_index->codes[payload[j]] = j + count_so_far;
            // TODO: add tkey associasted with this code
          } else {
            //std::cout << "dublicate error gather: " << k << " " << log_index->codes[payload[j]] << " " << j << " " << (void*)payload[j] << std::endl;
          }
        }
        count_so_far += res_count;
      }
      // free gather
      log[tkey]->finalize_states_log.clear();
    }      
    for (int i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      if (log.count(tkey) == 0 || log[tkey]->combine_log.empty()) continue;
      //std::cout << "combine states: " << log[tkey]->combine_log.size() << std::endl;
      for (int k=log[tkey]->combine_log.size()-1; k >= 0; --k) {
        idx_t res_count = log[tkey]->combine_log[k].count;
        auto src = log[tkey]->combine_log[k].src;
        auto target = log[tkey]->combine_log[k].target;
        for (idx_t j=0; j < res_count; ++j) {
          //std::cout << res_count << " combine: " << j << " " << log_index->codes[src[j]] << " " << log_index->codes[target[j]]  << " " << (void*)src[j] << " " << (void*)target[j] << std::endl;
          log_index->codes[src[j]] = log_index->codes[target[j]];
        }
      }
      // free combine
      log[tkey]->combine_log.clear();
    }      

    // std::cout << " done " << std::endl;
    break;
  }
	case PhysicalOperatorType::ORDER_BY: {
    // TODO: remove
    idx_t count_so_far = 0;
    for (int i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      if (log.count(tkey) == 0 || log[tkey]->reorder_log.empty()) continue;
      for (int k = 0; k < log[tkey]->reorder_log.size(); k++) {
        // std::cout << k << "Order : " << log[tkey]->reorder_log.size() << " " << count_so_far << " " << log[tkey]->reorder_log[k].size() << std::endl;
        idx_t res_count = log[tkey]->reorder_log[k].size();
        for (idx_t j=0; j < res_count; ++j) {
            log_index->vals.push_back( log[tkey]->reorder_log[k][j] + count_so_far );
        }
        count_so_far += res_count;
      }
      log[tkey]->reorder_log.clear();
    }
		break;
	}
	case PhysicalOperatorType::HASH_JOIN: {
    for (int i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      idx_t count_so_far = 0;
      if (log.count(tkey) == 0 || log[tkey]->scatter_sel_log.empty()) continue;
      // std::cout << "Scatter Join: " << log[tkey]->scatter_sel_log.size() << std::endl;
      for (int k = 0; k < log[tkey]->scatter_sel_log.size(); k++) {
        idx_t res_count = log[tkey]->scatter_sel_log[k].count;
        idx_t in_start = log[tkey]->scatter_sel_log[k].in_start;
        auto payload = log[tkey]->scatter_sel_log[k].addresses;
        //std::cout << k << " " << res_count << std::endl;
        if (log[tkey]->scatter_sel_log[k].sel) {
          auto sel = log[tkey]->scatter_sel_log[k].sel;
          for (idx_t j=0; j < res_count; ++j) {
            if (log_index->codes.find(payload[j]) == log_index->codes.end()) {
              log_index->codes[payload[j]] = sel[j] + in_start;
              //std::cout << "gather: " << k << " " << log_index->codes[payload[j]] << " " << j << " " << (void*)payload[j] << std::endl;
            }
          }
        } else {
          for (idx_t j=0; j < res_count; ++j) {
            if (log_index->codes.find(payload[j]) == log_index->codes.end()) {
              log_index->codes[payload[j]] = j + in_start;
              //std::cout << "gather: " << k << " " << log_index->codes[payload[j]] << " " << j << " " << (void*)payload[j] << std::endl;
            }
          }
        }
      }
    }
    for (int i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      if (log.count(tkey) == 0 || log[tkey]->perfect_full_scan_ht_log.empty()) continue;
     // std::cout << "Perfect Join: " << log[tkey]->perfect_full_scan_ht_log.size() << std::endl;
      for (int k = 0; k < log[tkey]->perfect_full_scan_ht_log.size(); k++) {
        idx_t key_count = log[tkey]->perfect_full_scan_ht_log[k].key_count;
        idx_t ht_count = log[tkey]->perfect_full_scan_ht_log[k].ht_count;
       //  std::cout << k << " " << key_count << " " << ht_count << std::endl;
        for (int e=0; e < key_count; e++) {
          idx_t build_idx = log[tkey]->perfect_full_scan_ht_log[k].sel_build->owned_data.get()[e];
          idx_t tuples_idx = log[tkey]->perfect_full_scan_ht_log[k].sel_tuples->owned_data.get()[e];
          data_ptr_t* ptr = (data_ptr_t*)log[tkey]->perfect_full_scan_ht_log[k].row_locations->GetData();
         // std::cout << "-> " << build_idx << " " << tuples_idx << " " << key_count << " " << ht_count  << std::endl;
         // TODO: check if this is correct. follow old implementation
         log_index->perfect_codes[build_idx] = log_index->codes[ptr[tuples_idx]];
         // std::cout << "-> " << (void*)ptr[ tuples_idx ]  << std::endl;
        }
      }
    }
    // std::cout << operator_id << " log_index: " << log_index->vals.size() << std::endl;
    break;
  }
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN: {
		break;
	}
	default: {
		// Lineage unimplemented! TODO all of these :)
	}
	}
	processed = true;
}

//! Get the column types for this operator
//! Returns 1 vector of ColumnDefinitions for each table that must be created
void OperatorLineage::GetTableColumnTypes(
    vector<LogicalType> &return_types, vector<string> &names) {
	vector<ColumnDefinition> source;
	switch (type) {
	case PhysicalOperatorType::COLUMN_DATA_SCAN:
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::TABLE_SCAN: {
    names.emplace_back("in_index");
    return_types.emplace_back(LogicalType::INTEGER);
    names.emplace_back("out_index");
    return_types.emplace_back(LogicalType::BIGINT);
    names.emplace_back("partition_index");
    return_types.emplace_back(LogicalType::INTEGER);
    break;
  }
	case PhysicalOperatorType::STREAMING_LIMIT:
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::ORDER_BY: {
    names.emplace_back("in_index");
    return_types.emplace_back(LogicalType::BIGINT);
    names.emplace_back("out_index");
    return_types.emplace_back(LogicalType::BIGINT);
    names.emplace_back("partition_index");
    return_types.emplace_back(LogicalType::INTEGER);
		break;
	}
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
    names.emplace_back("in_index");
    return_types.emplace_back(LogicalType::BIGINT);
    names.emplace_back("out_index");
    return_types.emplace_back(LogicalType::BIGINT);
    names.emplace_back("sink_index");
    return_types.emplace_back(LogicalType::INTEGER);
    names.emplace_back("getdata_index");
    return_types.emplace_back(LogicalType::INTEGER);
		break;
	}
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN: {
    names.emplace_back("lhs_index");
    return_types.emplace_back(LogicalType::INTEGER);
    names.emplace_back("rhs_index");
    return_types.emplace_back(LogicalType::INTEGER);
    names.emplace_back("out_index");
    return_types.emplace_back(LogicalType::BIGINT);
    names.emplace_back("sink_index");
    return_types.emplace_back(LogicalType::INTEGER);
    names.emplace_back("getdata_index");
    return_types.emplace_back(LogicalType::INTEGER);
		break;
	}
	case PhysicalOperatorType::HASH_JOIN:{
    names.emplace_back("lhs_index");
    return_types.emplace_back(LogicalType::BIGINT);
    names.emplace_back("rhs_index");
    return_types.emplace_back(LogicalType::BIGINT);
    names.emplace_back("out_index");
    return_types.emplace_back(LogicalType::BIGINT);
    names.emplace_back("sink_index");
    return_types.emplace_back(LogicalType::INTEGER);
    names.emplace_back("getdata_index");
    return_types.emplace_back(LogicalType::INTEGER);
		break;
	}
	default: {
		// Lineage unimplemented! TODO all of these :)
	}
	}
}

idx_t OperatorLineage::GetLineageAsChunk(DataChunk &insert_chunk,
                        idx_t& global_count, idx_t& local_count,
                        idx_t &thread_id, idx_t &data_idx,  bool &cache) {
	if (thread_vec.size() <= thread_id) {
		return 0;
	}

	void* thread_val  = thread_vec[thread_id];
	GetLineageAsChunkLocal(data_idx, global_count, local_count, insert_chunk, thread_id, log[thread_val]);

	global_count += insert_chunk.size();
	local_count += insert_chunk.size();
	data_idx++;

	if (insert_chunk.size() == 0) {
		thread_id++;
		cache = true;
    local_count = 0;
		data_idx = 0;
	}

	return insert_chunk.size();
}

void addOffset(sel_t* ptr, int count, int offset) {
  if (offset == 0 || ptr == NULL) return;
  for (idx_t j=0; j < count; ++j) {
    ptr[j] += offset;
  }
}

vector<idx_t> OperatorLineage::LQ_single(vector<idx_t>& log_context) {
  if (log_context.size() < 3) return {};
  idx_t thread_idx = log_context[0];
  idx_t lsn = log_context[1];
  idx_t oid = log_context[2];
	void* thread_val  = thread_vec[thread_idx];
  auto& tlog = log[thread_val];

	switch (type) {
	case PhysicalOperatorType::FILTER: {
    D_ASSERT(lsn < tlog->filter_log.size());
    int blsn = tlog->execute_internal[lsn].first-1;
    int branch = tlog->execute_internal[lsn].second;
    idx_t in_lsn = 0; // TODO: log->filter_log[lsn].in_lsn and use the same thread_id 
    idx_t count = 0;
    if (branch == 0) {
      count = tlog->filter_log[blsn].count;
      idx_t offset = tlog->filter_log[blsn].in_start;
      return { tlog->filter_log[blsn].sel[oid] }; // + offset to get global
    } else {
      idx_t offset = tlog->all_filter_log[lsn];
      return { oid }; // + offset to get global
    }
    break;
  } case PhysicalOperatorType::ORDER_BY: {
    D_ASSERT(lsn < tlog->reorder_log.size());
    // TODO: figure out how to recurse to the child
    return { tlog->reorder_log[lsn][oid] };
  } case PhysicalOperatorType::TABLE_SCAN: {
    D_ASSERT(lsn < tlog->row_group_log.size());
    int count = tlog->row_group_log[lsn].count;
    idx_t offset = tlog->row_group_log[lsn].start + tlog->row_group_log[lsn].vector_index;
    if (tlog->row_group_log[lsn].sel) {
      return { tlog->row_group_log[lsn].sel[oid] + offset };
    } else {
      return { oid + offset };
    }
  } case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
    // 1) getdata()
    D_ASSERT(lsn < tlog->finalize_states_log.size());
    idx_t res_count = tlog->finalize_states_log[lsn].count;
    auto payload = tlog->finalize_states_log[lsn].addresses;
    auto addr = payload[oid];
    
    // 2) combine()
    // look across all threads to see who wrote to payload[oid]
    // multiple threads could write to addr 
    vector<pair<idx_t, data_ptr_t>> partition_addr; 
    for (int i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      shared_ptr<Log>& tlog = log[tkey];
      for (int k=tlog->combine_log.size()-1; k >= 0; --k) {
        idx_t res_count = log[tkey]->combine_log[k].count;
        auto src = log[tkey]->combine_log[k].src;
        auto target = log[tkey]->combine_log[k].target;
        bool stop = false;
        for (idx_t j=0; j < res_count; ++j) {
          if (addr == target[j]) {
            partition_addr.push_back({i, target[k]}); 
            stop = true;
            break;
          }
        }
        if (stop) break;
      }
    }      

    // 3) sink()
    // 1:N
    // for each thread_id, addr in partition_addr, start from the end and find all matches
    vector<idx_t> result;
    for (auto& p : partition_addr) {
      idx_t thread_id = p.first;
      data_ptr_t addr = p.second;
      for (int i=0; i < thread_vec.size(); i++) {
        void* tkey = thread_vec[i];
        shared_ptr<Log>& tlog = log[tkey];
        idx_t total_count = 0;
        for (int k=0; k < tlog->int_scatter_log.size(); ++k) {
          idx_t count = tlog->int_scatter_log[k].count;
          int* payload = tlog->int_scatter_log[k].addresses;
          int tuple_size = tlog->tuple_size;
          uintptr_t fixed = tlog->fixed;
          for (idx_t j=0; j < count; ++j) {
            data_ptr_t key = (data_ptr_t)(fixed + payload[j] * tuple_size);
            if (key == p.second) {
              result.emplace_back(j + total_count);
            }
          }
          total_count = count;
        }
      }
    }
    return result;
  } default: {}	}
  
  return {};
}

vector<idx_t> OperatorLineage::ResolveGlobal(idx_t oid) {
  // iterate over all partitions
  // find the one oid IN its range. return: thread_idx, lsn, local_oid
  vector<idx_t> log_context;
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
          return {i, lsn, oid-offset};
        }
      }
    }      
    break;
  } case PhysicalOperatorType::ORDER_BY: {
    for (idx_t i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      shared_ptr<Log>& tlog = log[tkey];
      for (idx_t lsn=0; lsn  < tlog->reorder_log.size(); ++lsn) {
          if (tlog->reorder_log[lsn].size() > oid) {// e: vector<idx_t>
            return {i, lsn, oid};
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
          return {i, lsn, oid-offset};
        }
        offset += count;
      }
    }      
    break;
  } case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
    // 1) getdata()
    for (idx_t i=0; i < thread_vec.size(); i++) {
      idx_t offset = 0;
      void* tkey = thread_vec[i];
      shared_ptr<Log>& tlog = log[tkey];
      for (idx_t lsn=0; lsn  < tlog->finalize_states_log.size(); ++lsn) {
        idx_t count = tlog->finalize_states_log[lsn].count;
        if (oid > offset && oid < offset + count) {
          return {i, lsn, oid - offset};
        }
      }
    }
    break;
  } default: {}
	}
  
  return {};
}

idx_t OperatorLineage::GetLineageAsChunkLocal(idx_t data_idx, idx_t global_count, idx_t local_count,
    DataChunk& chunk, int thread_id, shared_ptr<Log> log) {
	if (log == nullptr) return 0;
  //std::cout << "get lineage as chunk : " << data_idx << " " << global_count << " " << thread_id <<std::endl;

  Vector thread_id_vec(Value::INTEGER(thread_id));
	switch (type) {
	// schema: [INTEGER in_index, INTEGER out_index, INTEGER partition_index]
	case PhysicalOperatorType::FILTER: {
    if (data_idx >= log->filter_log.size()) return 0;
    int lsn = log->execute_internal[data_idx].first-1;
    int branch = log->execute_internal[data_idx].second;
    // std::cout << "filter: " << data_idx << " " << lsn << std::endl;
    idx_t count = 0;
    idx_t offset = 0;
    data_ptr_t ptr = nullptr;
    if (branch == 0) {
      count = log->filter_log[lsn].count;
      offset = log->filter_log[lsn].in_start;
      if (log->filter_log[lsn].sel) {
        ptr = (data_ptr_t)log->filter_log[lsn].sel;
      }
    } else {
      count = branch;
      offset = log->all_filter_log[lsn];
    }
    chunk.SetCardinality(count);
    // TODO: in_index should reference an expression in_index + offset
    if (ptr != nullptr) {
      // TODO: add flag to log
      addOffset((sel_t*)ptr, count, offset);
      log->filter_log[lsn].in_start = 0;
      Vector in_index(LogicalType::INTEGER, ptr);
      chunk.data[0].Reference(in_index);
    } else {
      chunk.data[0].Sequence(offset, 1, count); // in_index
    }
    chunk.data[1].Sequence(global_count, 1, count); // out_index
    chunk.data[2].Reference(thread_id_vec);
    break;
  }
	case PhysicalOperatorType::TABLE_SCAN: {
    if (data_idx >= log->row_group_log.size()) return 0;
    idx_t count = log->row_group_log[data_idx].count;
    idx_t offset = log->row_group_log[data_idx].start + log->row_group_log[data_idx].vector_index;
    data_ptr_t ptr = nullptr;
    if (log->row_group_log[data_idx].sel) {
     // ptr = (data_ptr_t)log->row_group_log[data_idx].sel->owned_data.get();
      ptr = (data_ptr_t)log->row_group_log[data_idx].sel; //owned_data.get();
    }
    // std::cout << "TABLE_SCAN " << count << " " << offset << " " << global_count << std::endl;
    chunk.SetCardinality(count);
    if (ptr != nullptr) {
      addOffset((sel_t*)ptr, count, offset);
      log->row_group_log[data_idx].start = 0;
      log->row_group_log[data_idx].vector_index = 0;
      Vector in_index(LogicalType::INTEGER, ptr);
      chunk.data[0].Reference(in_index);
    } else {
      chunk.data[0].Sequence(offset, 1, count); // in_index
    }
    chunk.data[1].Sequence(global_count, 1, count); // out_index
    chunk.data[2].Reference(thread_id_vec);
    break;
  }
  case PhysicalOperatorType::STREAMING_LIMIT:
  case PhysicalOperatorType::LIMIT: {
    if (data_idx >= log->limit_offset.size()) return 0;

    idx_t start = log->limit_offset[data_idx].start;
    idx_t count = log->limit_offset[data_idx].end;
    idx_t offset = log->limit_offset[data_idx].in_start;
    chunk.SetCardinality(count);
    chunk.data[0].Sequence(start+offset, 1, count); // in_index
    chunk.data[1].Sequence(global_count, 1, count); // out_index
    chunk.data[2].Reference(thread_id_vec);
    break;
  }
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
  {  
    if (data_idx >= log->bnlj_log.size()) return 0;
    int lsn = log->execute_internal[data_idx].first-1;
    int branch = log->execute_internal[data_idx].second;
    // use cross product and bnlj_log
    idx_t branch_scan_lhs = log->cross_log[lsn].branch_scan_lhs;
    idx_t offset = log->cross_log[lsn].in_start;
    idx_t position_in_chunk = log->cross_log[lsn].position_in_chunk;
    idx_t scan_position = log->cross_log[lsn].scan_position;
    idx_t count = 0;
    Vector lhs_payload(LogicalType::INTEGER, count);
    Vector rhs_payload(Value::Value::INTEGER(scan_position + position_in_chunk));

    // TODO: fix and take account all branches
    count = log->bnlj_log[lsn].count;
    data_ptr_t left_ptr = (data_ptr_t)log->bnlj_log[lsn].sel;
    addOffset((sel_t*)left_ptr, count, offset);
    log->cross_log[lsn].in_start = 0;
    Vector temp(LogicalType::INTEGER, left_ptr);
    lhs_payload.Reference(temp);

    chunk.SetCardinality(count);
    chunk.data[0].Reference(lhs_payload);
    chunk.data[1].Reference(rhs_payload);
    chunk.data[2].Sequence(global_count, 1, count); // out_index
    chunk.data[3].Reference(thread_id_vec); // sink
    chunk.data[4].Reference(thread_id_vec); // get data
    break;
  } 
  case PhysicalOperatorType::PIECEWISE_MERGE_JOIN: // TODO: separate and apply soring lineage
	case PhysicalOperatorType::NESTED_LOOP_JOIN: {
    if (data_idx >= log->nlj_log.size()) return 0;
    int lsn = log->execute_internal[data_idx].first-1;
    idx_t count = log->nlj_log[lsn].count;
    chunk.SetCardinality(count);
    Vector lhs_payload(LogicalType::INTEGER, count);
    Vector rhs_payload(LogicalType::INTEGER, count);
    if (log->nlj_log[lsn].left) {
      data_ptr_t left_ptr = (data_ptr_t)log->nlj_log[lsn].left.get();
      idx_t offset = log->nlj_log[lsn].out_start;
      addOffset((sel_t*)left_ptr, count, offset);
      log->nlj_log[lsn].out_start = 0;
      Vector temp(LogicalType::INTEGER, left_ptr);
      lhs_payload.Reference(temp);
    } else {
      lhs_payload.SetVectorType(VectorType::CONSTANT_VECTOR);
      ConstantVector::SetNull(lhs_payload, true);
    }
    
    if (log->nlj_log[lsn].right) {
      data_ptr_t right_ptr = (data_ptr_t)log->nlj_log[lsn].right.get();
      idx_t offset = log->nlj_log[lsn].current_row_index;
      addOffset((sel_t*)right_ptr, count, offset);
      log->nlj_log[lsn].current_row_index = 0;
      Vector temp(LogicalType::INTEGER, right_ptr);
      rhs_payload.Reference(temp);
    } else {
      rhs_payload.SetVectorType(VectorType::CONSTANT_VECTOR);
      ConstantVector::SetNull(rhs_payload, true);
    }
    chunk.data[0].Reference(lhs_payload);
    chunk.data[1].Reference(rhs_payload);
    chunk.data[2].Sequence(global_count, 1, count); // out_index
    chunk.data[3].Reference(thread_id_vec); // sink
    chunk.data[4].Reference(thread_id_vec); // get data
    break;
  }
  case PhysicalOperatorType::CROSS_PRODUCT: {
    if (data_idx >= log->cross_log.size()) return 0;
    int lsn = log->execute_internal[data_idx].first-1;

    idx_t branch_scan_lhs = log->cross_log[lsn].branch_scan_lhs;
    idx_t count = log->cross_log[lsn].count;
    idx_t offset = log->cross_log[lsn].in_start;
    idx_t position_in_chunk = log->cross_log[lsn].position_in_chunk;
    idx_t scan_position = log->cross_log[lsn].scan_position;
    chunk.SetCardinality(count);
    
    if (branch_scan_lhs == false) {
      Vector rhs_payload(Value::Value::INTEGER(scan_position + position_in_chunk));
      Vector lhs_payload(LogicalType::INTEGER, count);
      lhs_payload.Sequence(offset, 1, count);
      chunk.data[0].Reference(lhs_payload);
      chunk.data[1].Reference(rhs_payload);
    } else {
      Vector rhs_payload(LogicalType::INTEGER, count);
      Vector lhs_payload(Value::Value::INTEGER(position_in_chunk + offset));
      rhs_payload.Sequence(scan_position, 1, count);
      chunk.data[0].Reference(lhs_payload);
      chunk.data[1].Reference(rhs_payload);
    }
    chunk.data[2].Sequence(global_count, 1, count); // out_index
    chunk.data[3].Reference(thread_id_vec); // sink
    chunk.data[4].Reference(thread_id_vec); // get data
    break;
  }
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
    if (data_idx >= log->int_scatter_log.size()) return 0;
    idx_t count = log->int_scatter_log[data_idx].count;
    chunk.SetCardinality(count);
    int* payload = log->int_scatter_log[data_idx].addresses;
    int tuple_size = log->tuple_size;
    uintptr_t fixed = log->fixed;
    chunk.data[1].Initialize(false, count);
    int64_t* out_index_ptr = (int64_t*)chunk.data[1].GetData();
    for (idx_t j=0; j < count; ++j) {
        data_ptr_t key = (data_ptr_t)(fixed + payload[j] * tuple_size);
        if (log_index->codes.find(key) == log_index->codes.end()) {
          std::cout << "gb probe: " <<  count <<  " " << (void*)payload[j] << std::endl;
        }
        out_index_ptr[j] = (int)log_index->codes[ key ];
    }
    chunk.data[0].Sequence(global_count, 1, count); // in_index
    chunk.data[2].Reference(thread_id_vec);
    chunk.data[3].Reference(thread_id_vec);
		break;
	}
	case PhysicalOperatorType::HASH_GROUP_BY: {
    if (data_idx >= log->scatter_log.size()) return 0;
    idx_t count = log->scatter_log[data_idx].count;
    chunk.SetCardinality(count);
    data_ptr_t* payload = log->scatter_log[data_idx].addresses;
    chunk.data[1].Initialize(false, count);
    int64_t* out_index_ptr = (int64_t*)chunk.data[1].GetData();
    for (idx_t j=0; j < count; ++j) {
        data_ptr_t key = payload[j];
        if (log_index->codes.find(key) == log_index->codes.end()) {
          std::cout << "gb probe: " <<  count <<  " " << (void*)payload[j] << std::endl;
        }
        out_index_ptr[j] = (int)log_index->codes[ key ];
    }
    chunk.data[0].Sequence(global_count, 1, count); // in_index
    chunk.data[2].Reference(thread_id_vec);
    chunk.data[3].Reference(thread_id_vec);
		break;
	}
	case PhysicalOperatorType::ORDER_BY: {
    if (log_index->offset >= log_index->vals.size()) return 0;
    idx_t count = log_index->vals.size() - log_index->offset;
    if (count > STANDARD_VECTOR_SIZE) {
      count = STANDARD_VECTOR_SIZE;
    }
    data_ptr_t ptr = (data_ptr_t)(log_index->vals.data() + log_index->offset);
    chunk.SetCardinality(count);
    Vector in_index(LogicalType::BIGINT, ptr);
    chunk.data[0].Reference(in_index); // in_index
    chunk.data[1].Sequence(global_count, 1, count); // out_index
    chunk.data[2].Reference(thread_id_vec);
    log_index->offset += count;
		break;
	}
	case PhysicalOperatorType::HASH_JOIN: {
    int out_count = 0;
    if (log != nullptr && !log->perfect_probe_ht_log.empty()) {
      if (data_idx >= log->perfect_probe_ht_log.size()) return 0;
      int lsn = log->execute_internal[data_idx].first-1;
      idx_t count = log->perfect_probe_ht_log[lsn].count;
      idx_t in_start = log->perfect_probe_ht_log[lsn].in_start;
      //std::cout << "perfect hash join: " << data_idx << " " << log_index->offset << " " << log->perfect_probe_ht_log.size() << " " << lsn << " " << in_start <<std::endl;
      
      idx_t lhs_col = 0;
      idx_t rhs_col = 1;
      chunk.SetCardinality(count);
      chunk.data[lhs_col].Initialize(false, count);
      chunk.data[rhs_col].Initialize(false, count);
      int64_t* lhs_col_data = (int64_t*)chunk.data[lhs_col].GetData();
      int64_t* rhs_col_data = (int64_t*)chunk.data[rhs_col].GetData();
      auto left = log->perfect_probe_ht_log[lsn].left;
      auto right = log->perfect_probe_ht_log[lsn].right;

      if (left == nullptr) {
        for (idx_t j=0; j < count; ++j) {
          rhs_col_data[j] = log_index->perfect_codes[ right[j] ];
          lhs_col_data[j] = j + in_start;
        }
      } else {
        for (idx_t j=0; j < count; ++j) {
          rhs_col_data[j] = log_index->perfect_codes[ right[j] ];
          lhs_col_data[j] = left[j] + in_start;
        }
      }
      log_index->offset += count;
      out_count = count;
    } else if (log != nullptr && !log->join_gather_log.empty()) {
      if (data_idx >= log->join_gather_log.size()) return 0;
      int lsn = log->execute_internal[data_idx].first-1;
      idx_t count = log->join_gather_log[lsn].count;
      idx_t in_start = log->join_gather_log[lsn].in_start;
      auto payload = log->join_gather_log[lsn].rhs;
      auto lhs = log->join_gather_log[lsn].lhs;
      
      idx_t lhs_col = 0;
      idx_t rhs_col = 1;
      chunk.SetCardinality(count);
      chunk.data[rhs_col].Initialize(false, count);
      int64_t* rhs_col_data = (int64_t*)chunk.data[rhs_col].GetData();

      if (lhs) {
        chunk.data[lhs_col].Initialize(false, count);
        int64_t* lhs_col_data = (int64_t*)chunk.data[lhs_col].GetData();
        for (idx_t j=0; j < count; ++j) {
          if (log_index->codes.find(payload[j]) == log_index->codes.end()) {
             //std::cout << "probe: " << j<< " " << lhs[j] << " " << count <<  " " << (void*)payload[j] << std::endl;
          }
          rhs_col_data[j] = log_index->codes[ payload[j] ];
          lhs_col_data[j] = lhs[j] + in_start;
        }
      } else {
        for (idx_t j=0; j < count; ++j) {
          if (log_index->codes.find(payload[j]) == log_index->codes.end()) {
            // std::cout << "probe: " << " null " << count <<  " " << (void*)payload[j] << std::endl;
          }
          rhs_col_data[j] = log_index->codes[ payload[j] ];
        }
        Vector lhs_payload(LogicalType::BIGINT);
        lhs_payload.SetVectorType(VectorType::CONSTANT_VECTOR);
        ConstantVector::SetNull(lhs_payload, true);
        chunk.data[0].Reference(lhs_payload);
      }
      log_index->offset += count;
      out_count = count;
    }
    
    chunk.data[2].Sequence(global_count, 1, out_count); // out_index
    chunk.data[3].Reference(thread_id_vec);
    chunk.data[4].Reference(thread_id_vec);
		break;
	}
	default: {
		// Not Implemented
	}
	}

	return chunk.size();
}

} // namespace duckdb
#endif
