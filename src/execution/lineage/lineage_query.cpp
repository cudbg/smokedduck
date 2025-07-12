#ifdef LINEAGE

#include "duckdb/execution/lineage/operator_lineage.hpp"

namespace duckdb {

// TODO: another level that calls Backward() for each operator and stitch the output together
// e.g agg -> filter -> table scan
// agg -> join -> table scans
// join -> join ->tables scans
// join -> agg -> tables scan

// global oid -> index -> lsn for local oid 
// return operator level lineage for a single output id
// given exact lsn (data_idx)
vector<vector<idx_t>> OperatorLineage::Backward(idx_t local_oid, idx_t data_idx,  shared_ptr<Log> log) {
  // 1:1 mapping = scan, filter, limit, order by, one side of join
  // 1:n mapping = aggregates
  vector<vector<idx_t>> output;
	switch (type) {
	// schema: [INTEGER in_index, INTEGER out_index, INTEGER partition_index]
	case PhysicalOperatorType::FILTER: {
    if (data_idx >= log->filter_log.size()) return {};
    int lsn = log->execute_internal[data_idx].first-1;
    int branch = log->execute_internal[data_idx].second;
    if (branch == 0) {
      idx_t count = log->filter_log[lsn].count;
      if (log->filter_log[lsn].sel && count > local_oid) {
        output[0].push_back(log->filter_log[lsn].sel[local_oid]); // todo: get input lsn to be used next
      }
    } else {
      idx_t count = branch;
      if (count > local_oid) {
        output[0].push_back(local_oid);
      }
    }
    break;
  } case PhysicalOperatorType::TABLE_SCAN: {
    if (data_idx >= log->row_group_log.size()) return {};
    idx_t count = log->row_group_log[data_idx].count;
    idx_t offset = log->row_group_log[data_idx].start + log->row_group_log[data_idx].vector_index;
    if (log->row_group_log[data_idx].sel && count > local_oid) {
      output[0].push_back(log->row_group_log[data_idx].sel[local_oid] + offset);
    } else if (count > local_oid) {
      output[0].push_back(local_oid + offset);
    }
    break;
  } case PhysicalOperatorType::STREAMING_LIMIT: {
  } case PhysicalOperatorType::LIMIT: {
    if (data_idx >= log->limit_offset.size()) return {};
    idx_t start = log->limit_offset[data_idx].start;
    idx_t count = log->limit_offset[data_idx].end;
    idx_t offset = log->limit_offset[data_idx].in_start;
    output[0].push_back(start+offset+local_oid);
    break;
  } case PhysicalOperatorType::ORDER_BY: {
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
  } case PhysicalOperatorType::BLOCKWISE_NL_JOIN: {
    break;
  } case PhysicalOperatorType::HASH_JOIN: {
    break;
  } case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
    output[0].push_back(local_oid);
    break;
  } case PhysicalOperatorType::HASH_GROUP_BY: {
  } default: {
		// Not Implemented
  }
	}
}

} // namespace duckdb
#endif
