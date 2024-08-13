//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/lineage/operator_lineage.hpp
//
//
//===----------------------------------------------------------------------===//

#ifdef LINEAGE
#pragma once

#include "duckdb/execution/lineage/log_data.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/parser/column_definition.hpp"
#include <iostream>
#include <mutex>

namespace duckdb {

enum class PhysicalOperatorType : uint8_t;

class OperatorLineage;


//! OperatorLineage
/*!
    OperatorLineage is xxx
*/
class OperatorLineage {
public:
	explicit OperatorLineage(int operator_id, PhysicalOperatorType type, string name)
	    : operator_id(operator_id),  processed(false), type(type), name(name), table_name(""), extra(""),
      out_start(0), out_end(0) {
        log_index = make_shared_ptr<LogIndex>();
      }

	vector<ColumnDefinition>  GetTableColumnTypes();

	idx_t GetLineageAsChunk(DataChunk &insert_chunk,
	                        idx_t& global_count, idx_t& local_count,
	                        idx_t &thread_id, idx_t &data_idx,  bool &cache);
	idx_t GetLineageAsChunkLocal(idx_t data_idx, idx_t global_count, idx_t local_count, DataChunk& chunk, int thread_id, shared_ptr<Log> log);

	void PostProcess();
  std::vector<int64_t> GatherStats();
  

public:
  int operator_id;
  bool processed;
  PhysicalOperatorType type;
  string name;
  string table_name;
  string extra;
  std::vector<shared_ptr<OperatorLineage>> children;
  std::unordered_map<void*, shared_ptr<Log>> log;
  std::vector<void*> thread_vec;
  shared_ptr<LogIndex> log_index;
  idx_t out_start;
  idx_t out_end;
  std::mutex glock;
};

} // namespace duckdb
#endif


