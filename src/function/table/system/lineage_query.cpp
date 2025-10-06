// 22: Mark Join                                      ?
// 18: (1.3) SEMI JOIN ?? HJ 3                        ?
// 16: opid16? SEMI / ANTI SEMI  (MARK JOIN)                     ?
//
// 21: 
// 20: 
// 19: (0.05) 0.0006
// 17: 
// 15: (0.16) 0.0003 TODO (agg, agg) DELIM JOIN?
// 14: 0.008
// 13: (index 1) 0.012
// 12: X -> (0.004) 0.007 | (0.05) 0.09
// 11  0.002
// 10: X -> (0.04) 0.0004 | (0.4) 0.006 
// 9: 1.3 -> (index: 0.05) 0.004 | (1) 0.06
// 8: 0.180 -> (index: 0.008) 0.01 | (index: 0.1) 0.03
// 7: 3.2 -> (index: 0.3) 0.003 | (3) 0.06
// 5: 0.280 -> (index: 0.01) 0.003 | (0.1) 0.04
// 6: 0.001
// 4: 
// 3: X -> (index: 0.02) 0.0006 | (0.4) 0.003
// 2:
// 1: (0.2) 0.02 | 0.2->                                  
#ifdef LINEAGE

#include "duckdb/execution/lineage/lineage_manager.hpp"
#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct LineageQueryBindData : public TableFunctionData {
  idx_t qid;
  int opid;
  shared_ptr<OperatorLineage> lop;
  unordered_map<idx_t, vector<vector<idx_t>>> out_per_source;
  unordered_map<idx_t, vector<vector<idx_t>>>::iterator source_iter;
  idx_t oid;

  void Initialize() {
    qid = -1;
    opid = -1;
  }
};

struct LineageQueryGlobalState : public GlobalTableFunctionState {
	LineageQueryGlobalState() : offset(0) {	}
	idx_t offset;
};

struct LineageQueryLocalState : public LocalTableFunctionState {
	LineageQueryLocalState() : outer_cur(0), inner_offset(0) {	}
  idx_t outer_cur;
  idx_t inner_offset;
};


static unique_ptr<FunctionData> LineageQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
  auto result = make_uniq<LineageQueryBindData>();
  if (input.inputs.size() != 3) {
    throw BinderException("lineage_query(qid:int, opid:int, oid:[uint])");
  }

  bool debug = false;
  result->qid = input.inputs[0].GetValue<int>();
  result->opid = input.inputs[1].GetValue<int>();
  result->oid = input.inputs[2].GetValue<int>();

  // read and initalize idx_t qid; idx_t opid;
  auto lop = lineage_manager->queryid_to_plan[result->qid];
  if (result->opid < 100)
    result->lop = GetLop(lop, result->opid);
  else
    result->lop = GetNextChild(lop);
  if (result->lop == nullptr) return std::move(result);
  if (debug) std::cout << "LQ: "<< result->opid << " " << result->qid << std::endl;
	

  clock_t start = clock();
  vector<idx_t> log_context = result->lop->ResolveGlobal(result->oid);
	clock_t end = clock();
  float resolve_time = ((float) end - start) / CLOCKS_PER_SEC;

  unordered_map<idx_t, vector<idx_t>> oids_per_lsn;
  if (!log_context.empty())  {
    // std::cout << " log context: " << log_context[0] << " " << log_context[1] << std::endl;
    oids_per_lsn[log_context[0]] = {log_context[1]};
  }
  start = clock();
  result->lop->LQ(oids_per_lsn, result->out_per_source);
	end = clock();
  float lq_time = ((float) end - start) / CLOCKS_PER_SEC;
  //std::cout << "out_per_source: " << result->out_per_source.size() <<
  // " resolve " << resolve_time << " lq: " << lq_time << std::endl;
  if (result->out_per_source.empty()) return std::move(result);

  if (debug) std::cout << "|source| = " << result->out_per_source.size() << std::endl;

  return_types.emplace_back(LogicalType::ROW_TYPE);
  names.emplace_back("oid");
  return_types.emplace_back(LogicalType::ROW_TYPE);
  names.emplace_back("table");
  return_types.emplace_back(LogicalType::ROW_TYPE);
  names.emplace_back("iid");
  
  result->source_iter = result->out_per_source.begin();

	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> LineageQueryGlobalInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<LineageQueryGlobalState>();
	return std::move(result);
}

unique_ptr<LocalTableFunctionState> LineageQueryLocalInit(ExecutionContext &context, TableFunctionInitInput &input,
                                                          GlobalTableFunctionState *global_state) {
	auto result = make_uniq<LineageQueryLocalState>();
	return std::move(result);
}

void LineageQueryFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
  auto &ldata = data_p.local_state->Cast<LineageQueryLocalState>();
  auto &gdata = data_p.global_state->Cast<LineageQueryGlobalState>();
  auto &bdata = data_p.bind_data->CastNoConst<LineageQueryBindData>();
  if (!lineage_manager || !bdata.lop || bdata.source_iter == bdata.out_per_source.end()) return; 
  
  Vector oid_vec(Value::BIGINT(bdata.oid));
  output.data[0].Reference(oid_vec);

  // second: idx_t, vector<vector<idx_t>>
  auto& inner = bdata.source_iter->second[ ldata.outer_cur ];
  // std::cout << bdata.source_iter->first << " " << ldata.outer_cur << " " << ldata.inner_offset << " " << inner.size() << " " <<  bdata.source_iter->second.size() << std::endl;
  if (ldata.inner_offset >= inner.size()) {
    ldata.outer_cur++;
    ldata.inner_offset = 0;
    if (ldata.outer_cur >= bdata.source_iter->second.size()) {
      bdata.source_iter++;
      ldata.outer_cur = 0;
    }
    if (bdata.source_iter == bdata.out_per_source.end()) return; 
    inner = bdata.source_iter->second[ ldata.outer_cur ];
  }

  Vector source_vec(Value::BIGINT(bdata.source_iter->first));
  output.data[1].Reference(source_vec);

  idx_t remaining = inner.size() - ldata.inner_offset;
  idx_t count = remaining  > STANDARD_VECTOR_SIZE ? STANDARD_VECTOR_SIZE : remaining;
  data_ptr_t ptr = (data_ptr_t)(inner.data() + ldata.inner_offset);
  Vector in_index(LogicalType::BIGINT, ptr);
  output.data[2].Reference(in_index); // in_index
  

  output.SetCardinality(count);
  ldata.inner_offset += count;
}

void LineageQueryFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("lineage_query",
        {duckdb::LogicalType::INTEGER, duckdb::LogicalType::INTEGER,
        LogicalType::UINTEGER},
        LineageQueryFunction, LineageQueryBind, LineageQueryGlobalInit, LineageQueryLocalInit));
}

} // namespace duckdb
#endif
