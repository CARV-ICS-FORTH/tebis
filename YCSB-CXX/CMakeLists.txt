# File: YCSB-CXX/CMakeLists.txt Author: Michalis Vardoulakis
# <mvard@ics.forth.gr>

include_directories(BEFORE core db ../tebis ../tcp_client)

if(Zookeeper_LIBRARY)
 # FIXME  fix the include directive paths in ycsb's subfolders

 add_executable(
      ycsb-tcp db/tcp_db.cc core/core_workload.cc driver/ycsbc.cc
  )

 target_link_libraries(
    ycsb-tcp
    tcp-client
 )

 add_executable(
     ycsb-blocking-tebis db/db_factory_blocking_tebis.cc core/core_workload.cc
                         driver/ycsbc.cc ../tebis_server/globals.c)

 target_link_libraries(
   ycsb-blocking-tebis
   tebis_client
   tebis_rdma
   parallax
   log
   libbloom
   ${Zookeeper_LIBRARY}
   ${cjson_LIBRARY})

   add_dependencies(ycsb-blocking-tebis cjson)
   set_target_properties(
   ycsb-blocking-tebis
    PROPERTIES LINK_FLAGS
               "-Wl,-no-as-needed -pthread -lm -lnuma -lrdmacm -libverbs -lrt"
               COMPILE_DEFINITIONS "tebis_DISTRIBUTED")

 add_executable(
     ycsb-async-tebis db/db_factory_async_tebis.cc core/core_workload.cc
                      driver/ycsbc.cc ../tebis_server/globals.c)

 add_dependencies(ycsb-async-tebis cjson)

 target_link_libraries(
   ycsb-async-tebis
   tebis_client
   tebis_rdma
   parallax
   log
   libbloom
   ${Zookeeper_LIBRARY}
   ${cjson_LIBRARY})

 set_target_properties(
    ycsb-async-tebis
    PROPERTIES LINK_FLAGS
               "-Wl,-no-as-needed -pthread -lm -lnuma -lrdmacm -libverbs -lrt"
               COMPILE_DEFINITIONS "tebis_DISTRIBUTED")

endif()
