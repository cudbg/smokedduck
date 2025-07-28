// 19: 0.0006
// 18: ?
// 16: opid16?
// 15: TODO (agg, agg)
// 14: 0.008
// 13: TOO expensive, nested agg
// 12: X -> (0.004) 0.007 | (0.05) 0.09
// 11  NLJ
// 10: X -> (0.04) 0.0004 | (0.4) 0.006 
// 9: 1.3 -> (index: 0.05) 0.004 | (1) 0.06
// 8: 0.180 -> (index: 0.008) 0.01 | (index: 0.1) 0.03
// 7: 3.2 -> (index: 0.3) 0.003 | (3) 0.06
// 5: 0.280 -> (index: 0.01) 0.003 | (0.1) 0.04
// 6: 0.001
// 3: X -> (index: 0.02) 0.0006 | (0.4) 0.003
// 1: 0.06 | 0.6-> ?
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
  idx_t oid;

  void Initialize() {
    qid = -1;
    opid = -1;
  }
};

struct LineageQueryGlobalState : public GlobalTableFunctionState {
	LineageQueryGlobalState() : offset(0) {
	}
	idx_t offset;
};

struct LineageQueryLocalState : public LocalTableFunctionState {
  idx_t cur;
  vector<vector<idx_t>> buffer;
};


static unique_ptr<FunctionData> LineageQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
  auto result = make_uniq<LineageQueryBindData>();
  if (input.inputs.size() != 3) {
    throw BinderException("lineage_query(qid:int, opid:int, oid:[uint])");
  }

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
  std::cout << "LQ: "<< result->opid << " " << result->qid << std::endl;
	

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

  return_types.emplace_back(LogicalType::ROW_TYPE);
  names.emplace_back("out");
  return_types.emplace_back(LogicalType::ROW_TYPE);
  names.emplace_back("in");
  
  // std::cout << "|source| = " << result->out_per_source.size() << std::endl;
 /* for (auto& source : result->out_per_source) {
    std::cout << "source: " << source.first << std::endl;
    for (auto& iids : source.second) {
      for (auto& id : iids) {
        std::cout << id << " ";
      }
    }
    std::cout << std::endl;
  }*/


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
  if (!lineage_manager || !bdata.lop) return; 
  idx_t oid = bdata.oid;
  Vector oid_vec(Value::BIGINT(oid));
  output.data[0].Reference(oid_vec);
  auto& out = bdata.out_per_source.begin()->second;
  if (ldata.cur >= out.size()) return;
  auto& cur_buffer = out[ldata.cur++];
  idx_t count = cur_buffer.size() > STANDARD_VECTOR_SIZE ? STANDARD_VECTOR_SIZE : cur_buffer.size();
  output.SetCardinality(count);
  data_ptr_t ptr = (data_ptr_t)(cur_buffer.data());
  Vector in_index(LogicalType::BIGINT, ptr);
  output.data[1].Reference(in_index); // in_index
}

void LineageQueryFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("lineage_query",
        {duckdb::LogicalType::INTEGER, duckdb::LogicalType::INTEGER,
        LogicalType::UINTEGER},
        LineageQueryFunction, LineageQueryBind, LineageQueryGlobalInit, LineageQueryLocalInit));
}

} // namespace duckdb
#endif
