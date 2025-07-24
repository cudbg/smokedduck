#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/parallel/thread_context.hpp"

#ifdef LINEAGE
#include "duckdb/execution/lineage/lineage_manager.hpp"
#endif

namespace duckdb {

PhysicalFilter::PhysicalFilter(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
                               idx_t estimated_cardinality)
    : CachingPhysicalOperator(PhysicalOperatorType::FILTER, std::move(types), estimated_cardinality) {
	D_ASSERT(select_list.size() > 0);
	if (select_list.size() > 1) {
		// create a big AND out of the expressions
		auto conjunction = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
		for (auto &expr : select_list) {
			conjunction->children.push_back(std::move(expr));
		}
		expression = std::move(conjunction);
	} else {
		expression = std::move(select_list[0]);
	}
}

class FilterState : public CachingOperatorState {
public:
	explicit FilterState(ExecutionContext &context, Expression &expr)
	    : executor(context.client, expr), sel(STANDARD_VECTOR_SIZE) {
	}

	ExpressionExecutor executor;
	SelectionVector sel;
#ifdef LINEAGE
  vector<idx_t> lineage;
  idx_t offset = 0;
#endif

public:
	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
		context.thread.profiler.Flush(op, executor, "filter", 0);
	}
};

unique_ptr<OperatorState> PhysicalFilter::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<FilterState>(context, *expression);
}

OperatorResultType PhysicalFilter::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                   GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = state_p.Cast<FilterState>();
	idx_t result_count = state.executor.SelectExpression(input, state.sel);

#ifdef LINEAGE
  if (lineage_manager && lineage_manager->smoke) {
    for (auto i=0; i < result_count; i++) {
      state.lineage.push_back(state.sel.get_index(i) + state.offset);
    }
    state.offset += result_count;
  }
#endif


	if (result_count == input.size()) {
#ifdef LINEAGE
    if (lineage_manager->capture && active_log) {
			active_log->filter_log.emplace_back(nullptr, result_count, active_log->in_start);
      active_log->latest.first = active_log->filter_log.size();
		}
#endif
		// nothing was filtered: skip adding any selection vectors
		chunk.Reference(input);
	} else {
#ifdef LINEAGE
    if (lineage_manager->capture && active_log && result_count) {
			sel_t* sel = (sel_t*) malloc(result_count * sizeof(sel_t));
			memcpy(sel, state.sel.data(),  result_count * sizeof(sel_t));
			active_log->filter_log.emplace_back(sel, result_count, active_log->in_start);
      active_log->latest.first = active_log->filter_log.size();
		}
#endif
		chunk.Slice(input, state.sel, result_count);
	}
	return OperatorResultType::NEED_MORE_INPUT;
}

string PhysicalFilter::ParamsToString() const {
	auto result = expression->GetName();
	result += "\n[INFOSEPARATOR]\n";
	result += StringUtil::Format("EC: %llu", estimated_cardinality);
	return result;
}

} // namespace duckdb
