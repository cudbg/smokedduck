add_library_unity(duckdb_lineage OBJECT lineage_manager.cpp log_data.cpp
                  operator_lineage.cpp lineage_query.cpp stats.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_lineage>
    PARENT_SCOPE)
