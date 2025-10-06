#ifdef LINEAGE

#include "duckdb/execution/lineage/lineage_manager.hpp"
#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct DuckDBQueriesListData : public GlobalTableFunctionState {
	DuckDBQueriesListData() : offset(0) {
	}

	idx_t offset;
};

static unique_ptr<FunctionData> DuckDBQueriesListBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("query_id");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("query");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("size_mb");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("tuples_count");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("nchunks");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("postprocess_time");
	return_types.emplace_back(LogicalType::FLOAT);
	
  names.emplace_back("build_time");
	return_types.emplace_back(LogicalType::FLOAT);

  names.emplace_back("plan");
	return_types.emplace_back(LogicalType::VARCHAR);


	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBQueriesListInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBQueriesListData>();
	return std::move(result);
}

static string JSONSanitize(const string &text) {
	string result;
	result.reserve(text.size());
	for (idx_t i = 0; i < text.size(); i++) {
		switch (text[i]) {
		case '\b':
			result += "\\b";
			break;
		case '\f':
			result += "\\f";
			break;
		case '\n':
			result += "\\n";
			break;
		case '\r':
			result += "\\r";
			break;
		case '\t':
			result += "\\t";
			break;
		case '"':
			result += "\\\"";
			break;
		case '\\':
			result += "\\\\";
			break;
		default:
			result += text[i];
			break;
		}
	}
	return result;
}

string PlanToString(shared_ptr<OperatorLineage> lop) {
  if (!lop) return "";
	string child_str;
 //std::cout << " ################## " << std::endl;
 //std::cout << lop->name << " " << lop->extra << std::endl;
	for (idx_t i = 0; i < lop->children.size(); i++) {
		child_str += PlanToString(lop->children[i]);
		if (i != lop->children.size() - 1) {
			child_str += ",";
		}
	}
	return "{\"name\": \"" + lop->name + "\", \"opid\": \"" + std::to_string(lop->operator_id) + "\", \"children\": [" + child_str + "],\"extra\": \"" + JSONSanitize(lop->extra)+ "\"}";
}

//! Create table to store executed queries with their IDs
//! Table name: queries_list
//! Schema: (INT query_id, varchar query)
void DuckDBQueriesListFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBQueriesListData>();
  if (!lineage_manager) return; 
	auto query_to_id = lineage_manager->query_to_id;
	if (data.offset >= query_to_id.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < query_to_id.size() && count < STANDARD_VECTOR_SIZE) {
    shared_ptr<OperatorLineage> lop = lineage_manager->queryid_to_plan[data.offset];
    std::vector<int64_t> stats = lineage_manager->GetStats(lop);
		string query = query_to_id[data.offset];
		idx_t col = 0;
		// query_id, INT
		output.SetValue(col++, count,Value::INTEGER(data.offset));
		// query, VARCHAR
		output.SetValue(col++, count, query);

    // size_mb
		output.SetValue(col++, count,Value::BIGINT(stats[0]));

    // count_mb
		output.SetValue(col++, count,Value::BIGINT(stats[1]));

    // nchunks
		output.SetValue(col++, count,Value::BIGINT(stats[2]));

    // postprocess_time
		clock_t start = clock();
		lineage_manager->PostProcess(lop);
		clock_t end = clock();
    float postprocess_time = ((float) end - start) / CLOCKS_PER_SEC;
		output.SetValue(col++, count,Value::FLOAT(postprocess_time));

    start = clock();
    if (lop) lop->BuildIndexes();
    end = clock();
    float build_time = ((float) end - start) / CLOCKS_PER_SEC;
		output.SetValue(col++, count,Value::FLOAT(build_time));

    // plan, VARCHAR
		output.SetValue(col++, count, PlanToString(lop));

		count++;
		data.offset++;
	}
	output.SetCardinality(count);
}

void DuckDBQueriesListFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_queries_list", {}, DuckDBQueriesListFunction, DuckDBQueriesListBind, DuckDBQueriesListInit));
}

} // namespace duckdb
#endif
