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
  result->lop = GetLop(lop, result->opid);
  if (result->lop == nullptr) return std::move(result);
  std::cout << "lineage query "<< result->opid << " " << result->qid << std::endl;

  return_types.emplace_back(LogicalType::ROW_TYPE);
  names.emplace_back("in");
  
  return_types.emplace_back(LogicalType::ROW_TYPE);
  names.emplace_back("out");

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
  // TODO: distribute oids to different locals
  idx_t oid = bdata.oid;
  if (ldata.cur == 0) {
    // 1) resolve global oid: (lsn, thread_id) -> oid
    vector<idx_t> log_context = bdata.lop->ResolveGlobal(oid);
    unordered_map<idx_t, vector<idx_t>> oids_per_lsn;
    oids_per_lsn[log_context[0]] = {log_context[1]};
    vector<vector<idx_t>> out;
    bdata.lop->LQ(oids_per_lsn, out);
    if (out.empty()) return;
    ldata.buffer = std::move(out);
  }
  
  if (ldata.cur >= ldata.buffer.size()) return;

  auto& cur_buffer = ldata.buffer[ldata.cur++];
  idx_t count = cur_buffer.size() > STANDARD_VECTOR_SIZE ? STANDARD_VECTOR_SIZE : cur_buffer.size();
  output.SetCardinality(count);
  data_ptr_t ptr = (data_ptr_t)(cur_buffer.data());
  Vector in_index(LogicalType::BIGINT, ptr);
  output.data[0].Reference(in_index); // in_index

  Vector oid_vec(Value::BIGINT(oid));
  output.data[1].Reference(oid_vec);
}

void LineageQueryFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("lineage_query",
        {duckdb::LogicalType::INTEGER, duckdb::LogicalType::INTEGER,
        LogicalType::UINTEGER},
        LineageQueryFunction, LineageQueryBind, LineageQueryGlobalInit, LineageQueryLocalInit));
}

} // namespace duckdb
#endif
