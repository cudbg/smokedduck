#include "duckdb/function/pragma/pragma_functions.hpp"

#include "duckdb/common/enums/output_type.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/logging/http_logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"

#ifdef LINEAGE
#include "duckdb/execution/lineage/lineage_manager.hpp"
#endif

#include <cctype>

namespace duckdb {

static void PragmaEnableProfilingStatement(ClientContext &context, const FunctionParameters &parameters) {
	auto &config = ClientConfig::GetConfig(context);
	config.enable_profiler = true;
	config.emit_profiler_output = true;
}

void RegisterEnableProfiling(BuiltinFunctions &set) {
	PragmaFunctionSet functions("");
	functions.AddFunction(PragmaFunction::PragmaStatement(string(), PragmaEnableProfilingStatement));

	set.AddFunction("enable_profile", functions);
	set.AddFunction("enable_profiling", functions);
}

static void PragmaDisableProfiling(ClientContext &context, const FunctionParameters &parameters) {
	auto &config = ClientConfig::GetConfig(context);
	config.enable_profiler = false;
}

static void PragmaEnableProgressBar(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).enable_progress_bar = true;
}

static void PragmaDisableProgressBar(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).enable_progress_bar = false;
}

static void PragmaEnablePrintProgressBar(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).print_progress_bar = true;
}

static void PragmaDisablePrintProgressBar(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).print_progress_bar = false;
}

static void PragmaEnableVerification(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).query_verification_enabled = true;
	ClientConfig::GetConfig(context).verify_serializer = true;
}

static void PragmaDisableVerification(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).query_verification_enabled = false;
	ClientConfig::GetConfig(context).verify_serializer = false;
}

static void PragmaVerifySerializer(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).verify_serializer = true;
}

static void PragmaDisableVerifySerializer(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).verify_serializer = false;
}

static void PragmaEnableExternalVerification(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).verify_external = true;
}

static void PragmaDisableExternalVerification(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).verify_external = false;
}

static void PragmaEnableFetchRowVerification(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).verify_fetch_row = true;
}

static void PragmaDisableFetchRowVerification(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).verify_fetch_row = false;
}

static void PragmaEnableForceParallelism(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).verify_parallelism = true;
}

static void PragmaForceCheckpoint(ClientContext &context, const FunctionParameters &parameters) {
	DBConfig::GetConfig(context).options.force_checkpoint = true;
}

static void PragmaDisableForceParallelism(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).verify_parallelism = false;
}

static void PragmaEnableObjectCache(ClientContext &context, const FunctionParameters &parameters) {
	DBConfig::GetConfig(context).options.object_cache_enable = true;
}

static void PragmaDisableObjectCache(ClientContext &context, const FunctionParameters &parameters) {
	DBConfig::GetConfig(context).options.object_cache_enable = false;
}

static void PragmaEnableCheckpointOnShutdown(ClientContext &context, const FunctionParameters &parameters) {
	DBConfig::GetConfig(context).options.checkpoint_on_shutdown = true;
}

static void PragmaDisableCheckpointOnShutdown(ClientContext &context, const FunctionParameters &parameters) {
	DBConfig::GetConfig(context).options.checkpoint_on_shutdown = false;
}

static void PragmaEnableOptimizer(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).enable_optimizer = true;
}

static void PragmaDisableOptimizer(ClientContext &context, const FunctionParameters &parameters) {
	ClientConfig::GetConfig(context).enable_optimizer = false;
}

#ifdef LINEAGE
static void PragmaEnableSmoke(ClientContext &context, const FunctionParameters &parameters) {
  if (!lineage_manager) lineage_manager = make_uniq<LineageManager>();
	lineage_manager->smoke = true;
	lineage_manager->capture = false;
	std::cout << "\nEnable Smoke Capture" << std::endl;
}

static void PragmaDisableSmoke(ClientContext &context, const FunctionParameters &parameters) {
  if (!lineage_manager) lineage_manager = make_uniq<LineageManager>();
	lineage_manager->smoke = false;
	lineage_manager->capture = false;
	std::cout << "\nDisable Smoke Capture" << std::endl;
}

static void PragmaEnableLineage(ClientContext &context, const FunctionParameters &parameters) {
  if (!lineage_manager) lineage_manager = make_uniq<LineageManager>();
	lineage_manager->capture = true;
	std::cout << "\nEnable Lineage Capture" << std::endl;
}

static void PragmaDisableLineage(ClientContext &context, const FunctionParameters &parameters) {
  if (lineage_manager) {
    lineage_manager->capture = false;
  }
	std::cout << "\nDisable Lineage Capture" << std::endl;
}

static void PragmaClearLineage(ClientContext &context, const FunctionParameters &parameters) {
  if (lineage_manager) {
    lineage_manager->Clear();
  }
	std::cout << "\nClear Lineage" << std::endl;
}

static void PragmaDisableFilterPushDown(ClientContext &context, const FunctionParameters &parameters) {
  if (lineage_manager) lineage_manager->enable_filter_pushdown = false;
  std::cout << "Disable Filter Pushdown" << std::endl;
}
static void PragmaEnableFilterPushDown(ClientContext &context, const FunctionParameters &parameters) {
  if (lineage_manager) lineage_manager->enable_filter_pushdown = true;
	std::cout << "Enable Filter Pushdown" << std::endl;
}

static void PragmaDisableShort(ClientContext &context, const FunctionParameters &parameters) {
  if (lineage_manager) lineage_manager->enable_short = false;
  std::cout << "Disable Short Circuiting" << std::endl;
}
static void PragmaEnableShort(ClientContext &context, const FunctionParameters &parameters) {
  if (lineage_manager) lineage_manager->enable_short = true;
	std::cout << "Enable Short Circuiting" << std::endl;
}
static void PragmaSetJoin(ClientContext &context, const FunctionParameters &parameters) {
	string join_type = parameters.values[0].ToString();
	D_ASSERT(join_type == "hash" || join_type == "merge" || join_type == "nl" || join_type == "index" || join_type == "block" || join_type == "clear");
	std::cout << "Setting join type to " << join_type << " - be careful! Failures possible for hash/index join if non equijoin." << std::endl;
  if (!lineage_manager) return;
	if (join_type == "clear") {
	  lineage_manager->explicit_join_type = nullptr;
	} else {
		lineage_manager->explicit_join_type = make_uniq<string>(join_type);
	}
}

static void PragmaSetAgg(ClientContext &context, const FunctionParameters &parameters) {
	string agg_type = parameters.values[0].ToString();
	D_ASSERT(agg_type == "perfect" || agg_type == "reg" || agg_type == "clear");
	std::cout << "Setting agg type to " << agg_type << " - be careful! Failures possible if too many buckets (I think)." << std::endl;
  if (!lineage_manager) return;
	if (agg_type == "clear") {
		lineage_manager->explicit_agg_type = nullptr;
	} else {
		lineage_manager->explicit_agg_type = make_uniq<string>(agg_type);
	}
}
#endif

void PragmaFunctions::RegisterFunction(BuiltinFunctions &set) {
	RegisterEnableProfiling(set);

	set.AddFunction(PragmaFunction::PragmaStatement("disable_profile", PragmaDisableProfiling));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_profiling", PragmaDisableProfiling));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_verification", PragmaEnableVerification));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_verification", PragmaDisableVerification));

	set.AddFunction(PragmaFunction::PragmaStatement("verify_external", PragmaEnableExternalVerification));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_verify_external", PragmaDisableExternalVerification));

	set.AddFunction(PragmaFunction::PragmaStatement("verify_fetch_row", PragmaEnableFetchRowVerification));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_verify_fetch_row", PragmaDisableFetchRowVerification));

	set.AddFunction(PragmaFunction::PragmaStatement("verify_serializer", PragmaVerifySerializer));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_verify_serializer", PragmaDisableVerifySerializer));

	set.AddFunction(PragmaFunction::PragmaStatement("verify_parallelism", PragmaEnableForceParallelism));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_verify_parallelism", PragmaDisableForceParallelism));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_object_cache", PragmaEnableObjectCache));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_object_cache", PragmaDisableObjectCache));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_optimizer", PragmaEnableOptimizer));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_optimizer", PragmaDisableOptimizer));

	set.AddFunction(PragmaFunction::PragmaStatement("force_checkpoint", PragmaForceCheckpoint));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_progress_bar", PragmaEnableProgressBar));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_progress_bar", PragmaDisableProgressBar));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_print_progress_bar", PragmaEnablePrintProgressBar));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_print_progress_bar", PragmaDisablePrintProgressBar));

	set.AddFunction(PragmaFunction::PragmaStatement("enable_checkpoint_on_shutdown", PragmaEnableCheckpointOnShutdown));
	set.AddFunction(
	    PragmaFunction::PragmaStatement("disable_checkpoint_on_shutdown", PragmaDisableCheckpointOnShutdown));
#ifdef LINEAGE
	set.AddFunction(PragmaFunction::PragmaStatement("enable_smoke", PragmaEnableSmoke));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_smoke", PragmaDisableSmoke));
	set.AddFunction(PragmaFunction::PragmaStatement("enable_lineage", PragmaEnableLineage));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_lineage", PragmaDisableLineage));
	set.AddFunction(PragmaFunction::PragmaStatement("clear_lineage", PragmaClearLineage));
	set.AddFunction(PragmaFunction::PragmaStatement("enable_filter_pushdown", PragmaEnableFilterPushDown));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_filter_pushdown", PragmaDisableFilterPushDown));
	set.AddFunction(PragmaFunction::PragmaStatement("enable_short", PragmaEnableShort));
	set.AddFunction(PragmaFunction::PragmaStatement("disable_short", PragmaDisableShort));
	set.AddFunction(PragmaFunction::PragmaCall("set_join", PragmaSetJoin, {LogicalType::VARCHAR}));
	set.AddFunction(PragmaFunction::PragmaCall("set_agg", PragmaSetAgg, {LogicalType::VARCHAR}));
#endif
}

} // namespace duckdb
