#ifdef LINEAGE

#include "duckdb/execution/lineage/lineage_manager.hpp"
#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct LineageViewBindData : public TableFunctionData {
  idx_t qid;
  int opid;
  shared_ptr<OperatorLineage> lop;

  void Initialize() {
    qid = -1;
    opid = -1;
  }
};

struct LineageViewGlobalState : public GlobalTableFunctionState {
	LineageViewGlobalState() : offset(0) {
	}
	idx_t offset;
};

struct LineageViewLocalState : public LocalTableFunctionState {
  idx_t global_count = 0;
	idx_t log_id = 0;
	idx_t current_thread = 0;
	idx_t local_count = 0;
	idx_t chunk_index = 0;
};

static unique_ptr<FunctionData> LineageViewBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
  auto result = make_uniq<LineageViewBindData>();
  if (input.inputs.size() != 2) {
    throw BinderException("lineage_view(qid:int, opid:int)");
  }

  result->qid = input.inputs[0].GetValue<int>();
  result->opid = input.inputs[1].GetValue<int>();

  // read and initalize idx_t qid; idx_t opid;
  auto lop = lineage_manager->queryid_to_plan[result->qid];
  result->lop = GetLop(lop, result->opid);
  if (result->lop == nullptr) return std::move(result);
  std::cout << "lineage view "<< result->opid << " " << result->qid << std::endl;

  // TODO: find the lop for opid
  result->lop->PostProcess();
  result->lop->GetTableColumnTypes(return_types, names);
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> LineageViewGlobalInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<LineageViewGlobalState>();
	return std::move(result);
}

unique_ptr<LocalTableFunctionState> LineageViewLocalInit(ExecutionContext &context, TableFunctionInitInput &input,
                                                          GlobalTableFunctionState *global_state) {
	auto result = make_uniq<LineageViewLocalState>();
	return std::move(result);
}

void LineageViewFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
  auto &bdata = data_p.bind_data->CastNoConst<LineageViewBindData>();
  auto &gdata = data_p.global_state->Cast<LineageViewGlobalState>();
  auto &ldata = data_p.local_state->Cast<LineageViewLocalState>();
  if (!lineage_manager || !bdata.lop) return; 
	output.Reset();
 	bool cache_on = false;
  do {
    cache_on = false;
    bdata.lop->GetLineageAsChunk(output, ldata.global_count, ldata.local_count,
                                        ldata.current_thread, ldata.log_id, cache_on);
  } while (cache_on && output.size() == 0);
}

void LineageViewFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("lineage_view", {duckdb::LogicalType::INTEGER, duckdb::LogicalType::INTEGER},
                   LineageViewFunction, LineageViewBind, LineageViewGlobalInit, LineageViewLocalInit));
}

} // namespace duckdb
#endif
