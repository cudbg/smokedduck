#ifdef LINEAGE

#include "duckdb/execution/lineage/lineage_manager.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/execution/operator/helper/physical_result_collector.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"

namespace duckdb {

unique_ptr<LineageManager> lineage_manager;
thread_local Log* active_log;

Log* LineageManager::InitLog(shared_ptr<OperatorLineage> lop, void* thread_id, Log* child_log) {
  if (!capture || !lop) return nullptr;

  std::lock_guard<std::mutex> lock(lop->glock);
  if (lop->log.count(thread_id) == 0) {
  //  std::cout << lop->operator_id << " " << thread_id << std::endl;
    auto log = make_uniq<Log>();
    if (child_log) log->child_log = child_log;
    lop->log[thread_id] = std::move(log);
    if (lop->type == PhysicalOperatorType::RIGHT_DELIM_JOIN || lop->type == PhysicalOperatorType::LEFT_DELIM_JOIN) {
		  for (auto c : lop->children) {
        std::lock_guard<std::mutex> lock(c->glock);
        if (c->log.count(thread_id) == 0) {
          c->log[thread_id] = make_uniq<Log>();
        }
      }
    }
  } else {
    if (child_log) lop->log[thread_id]->child_log = child_log;
  }
  return lop->log[thread_id].get();
}

shared_ptr<OperatorLineage> LineageManager::CreateOperatorLineage(ClientContext &context, PhysicalOperator *op) {
	global_logger[(void*)op] = make_shared_ptr<OperatorLineage>(op, operators_ids[(void*)op], op->type, op->GetName());
	op->lop = global_logger[(void*)op];
	InitLog(op->lop, (void*)&context);
  
  if (op->type == PhysicalOperatorType::RESULT_COLLECTOR) {
		PhysicalOperator* plan = &dynamic_cast<PhysicalResultCollector*>(op)->plan;
		shared_ptr<OperatorLineage> lop = CreateOperatorLineage(context, plan);
		global_logger[(void*)op]->children.push_back(lop);
	}

  if (op->type == PhysicalOperatorType::RIGHT_DELIM_JOIN || op->type == PhysicalOperatorType::LEFT_DELIM_JOIN) {
    idx_t cid = 0;
    if (op->type == PhysicalOperatorType::RIGHT_DELIM_JOIN) {
      cid = 1;
    }
    std::cout <<  op->ToString() << std::endl;
		auto distinct = (PhysicalOperator*)dynamic_cast<PhysicalDelimJoin *>(op)->distinct.get();
		shared_ptr<OperatorLineage> distinct_lop = CreateOperatorLineage(context, distinct);
		shared_ptr<OperatorLineage> join_lop = CreateOperatorLineage(context, dynamic_cast<PhysicalDelimJoin *>(op)->join.get());
    //std::cout << " ===== DISTINCT ======= " << distinct_lop->children.size() << std::endl;
    //std::cout <<  distinct->ToString() << std::endl;
    //std::cout << " ===== JOIN  ======= " << join_lop->children.size() <<  std::endl;
    //std::cout << dynamic_cast<PhysicalDelimJoin *>(op)->join.get()->ToString() << std::endl;
    
    join_lop->children[cid] = CreateOperatorLineage(context, op->children.back().get());
    distinct_lop->children.push_back(join_lop->children[cid]);
    op->lop->children.push_back(join_lop);
		
    for (idx_t i = 0; i < dynamic_cast<PhysicalDelimJoin *>(op)->delim_scans.size(); ++i) {
		  auto dscan = dynamic_cast<PhysicalDelimJoin *>(op)->delim_scans[i];
		  global_logger[(void*)&dscan.get()]->children.push_back(distinct_lop);
		}
	} else {
    for (idx_t i = 0; i < op->children.size(); i++) {
      shared_ptr<OperatorLineage> lop = CreateOperatorLineage(context, op->children[i].get());
      global_logger[(void*)op]->children.push_back(lop);
    }
    
  }



	return global_logger[(void*)op];
}

// Iterate through in Postorder to ensure that children have PipelineLineageNodes set before parents
int LineageManager::PlanAnnotator(PhysicalOperator *op, int counter) {
  if (op->type == PhysicalOperatorType::RESULT_COLLECTOR) {
		PhysicalOperator* plan = &dynamic_cast<PhysicalResultCollector*>(op)->plan;
    //std::cout << plan->ToString() << std::endl;
		counter = PlanAnnotator(plan, counter);
	}

  if (op->type == PhysicalOperatorType::RIGHT_DELIM_JOIN || op->type == PhysicalOperatorType::LEFT_DELIM_JOIN) {
		counter = PlanAnnotator( dynamic_cast<PhysicalDelimJoin *>(op)->join.get(), counter);
		counter = PlanAnnotator((PhysicalOperator*) dynamic_cast<PhysicalDelimJoin *>(op)->distinct.get(), counter);
	}

	for (idx_t i = 0; i < op->children.size(); i++) {
		counter = PlanAnnotator(op->children[i].get(), counter);
	}

	operators_ids[(void*)op] = counter;
	return counter + 1;
}

void LineageManager::InitOperatorPlan(ClientContext &context, PhysicalOperator *op) {
	if (!capture) return;
	PlanAnnotator(op, 0);
	CreateOperatorLineage(context, op);
}

void LineageManager::StoreQueryLineage(ClientContext &context, PhysicalOperator *op, string query) {
	if (!capture)
		return;

	idx_t query_id = query_to_id.size();
	query_to_id.push_back(query);
	queryid_to_plan[query_id] = lineage_manager->global_logger[(void *)op];
  active_log = nullptr;
}

void LineageManager::PostProcess(shared_ptr<OperatorLineage> lop) {
  if (lop == nullptr) return;
  if (lop->type == PhysicalOperatorType::DELIM_SCAN) return;
  lop->PostProcess();
	for (idx_t i = 0; i < lop->children.size(); i++) {
	  PostProcess(lop->children[i]);
	}
}

std::vector<int64_t> LineageManager::GetStats(shared_ptr<OperatorLineage> lop) {
  if (lop == nullptr) return {0, 0, 0};
  if (lop->type == PhysicalOperatorType::DELIM_SCAN) return {0, 0, 0};

  std::vector<int64_t> stats = lop->GatherStats();
  int64_t lineage_size_mb = stats[0];
  int64_t count = stats[1];
  int64_t nchunks = stats[2];
	
  for (idx_t i = 0; i < lop->children.size(); i++) {
    std::vector<int64_t> sub_stats = GetStats(lop->children[i]);
    lineage_size_mb += sub_stats[0];
    count += sub_stats[1];
    nchunks += sub_stats[2];
	}

  return {lineage_size_mb, count, nchunks};
}
  
void LineageManager::SetP(OperatorLineage* lop, void* thread_id) {
		if (!capture || !lop) return;
    if (!lop->children.empty()) SetChild(lop->children[0].get(), thread_id);
		std::lock_guard<std::mutex> lock(lop->glock);
		active_log = lop->log[thread_id].get();
    return;
}

// TODO: log per pipeline lop[op]
void LineageManager::SetChild(OperatorLineage* lop, void* thread_id) {
		if (!capture || lop) return;
		std::lock_guard<std::mutex> lock(lop->glock);
    auto it = lop->log.find(thread_id);
    if (it != lop->log.end()) {
        active_log = it->second.get();
    }
    return;
}

} // namespace duckdb
#endif
