# File: YCSB-CXX/CMakeLists.txt Author: Michalis Vardoulakis
# <mvard@ics.forth.gr>

include_directories(BEFORE core db ../kreon ../kreon_server)

if(Zookeeper_LIBRARY)
  # FIXME  fix the include directive paths in ycsb's subfolders

  add_executable(
    ycsb-blocking-kreon db/db_factory_blocking_kreonR.cc core/core_workload.cc
                        driver/ycsbc.cc ../kreon_server/globals.c)

  target_link_libraries(
    ycsb-blocking-kreon
    kreon_client
    kreon_rdma
    kreonr
    log
    libbloom
    ${Zookeeper_LIBRARY}
    ${cjson_LIBRARY})
  add_dependencies(ycsb-blocking-kreon cjson)
  set_target_properties(
    ycsb-blocking-kreon
    PROPERTIES LINK_FLAGS
               "-Wl,-no-as-needed -pthread -lm -lnuma -lrdmacm -libverbs"
               COMPILE_DEFINITIONS "KREON_DISTRIBUTED")

  add_executable(
    ycsb-async-kreon db/db_factory_async_kreonR.cc core/core_workload.cc
                     driver/ycsbc.cc ../kreon_server/globals.c)
  add_dependencies(ycsb-async-kreon cjson)
  target_link_libraries(
    ycsb-async-kreon
    kreon_client
    kreon_rdma
    kreonr
    log
    libbloom
    ${Zookeeper_LIBRARY}
    ${cjson_LIBRARY})
  set_target_properties(
    ycsb-async-kreon
    PROPERTIES LINK_FLAGS
               "-Wl,-no-as-needed -pthread -lm -lnuma -lrdmacm -libverbs"
               COMPILE_DEFINITIONS "KREON_DISTRIBUTED")

endif()

add_executable(ycsb-edb db/db_factory_eutropiadb.cc core/core_workload.cc
                        driver/ycsbc.cc ../utilities/list.c)

target_link_libraries(ycsb-edb kreon libbloom ${Zookeeper_LIBRARY})
set_target_properties(ycsb-edb PROPERTIES LINK_FLAGS "-pthread -lnuma -lrt")
