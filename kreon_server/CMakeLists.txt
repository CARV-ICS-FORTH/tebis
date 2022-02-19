set_source_files_properties(../utilities/spin_loop.c PROPERTIES COMPILE_FLAGS
                                                                "-O0")
add_definitions(-DKREONR)

add_library(
  kreon_client
  messages.c ../kreon_rdma_client/client_utils.c
  ../kreon_rdma_client/kreon_rdma_client.c ../kreon_server/globals.c zk_utils.c
  djb2.c)
add_dependencies(kreon_client cjson)

target_link_libraries(kreon_client ${Zookeeper_LIBRARY})

add_executable(
  kreon_server
  server.c
  metadata_server.c
  remote_compaction.c
  deserialize_index.c
  zk_utils.c
  djb2.c
  globals.c
  messages.c
  server_communication.c
  ../kreon_rdma/rdma.c
  ../kreon_rdma_client/client_utils.c
  stats.c)

add_dependencies(kreon_server cjson)
target_link_options(
  kreon_server
  PUBLIC
  -pthread
  -lnuma
  -libverbs
  -lrdmacm
  -lrt
  -lm)
target_link_libraries(kreon_server kreon_rdma kreonr log ${Zookeeper_LIBRARY}
                      ${cjson_LIBRARY})

add_executable(create_region create_region.c zk_utils.c
                             ../utilities/spin_loop.c)

target_link_libraries(create_region log ${Zookeeper_LIBRARY})
target_link_options(create_region PUBLIC -gdwarf-4 -pthread)

add_executable(create_server_node create_server_node.c zk_utils.c
                                  ../utilities/spin_loop.c)
target_link_libraries(create_server_node log ${Zookeeper_LIBRARY})
target_link_options(create_server_node PUBLIC -gdwarf-4 -pthread)

set_target_properties(
  kreon_client PROPERTIES PUBLIC_HEADER
                          "../kreon_rdma_client/kreon_rdma_client.h")

set(KREON_CLIENT_LIB "kreon_client")
set(KREON_SERVER_LIB "kreon_server")
if(KREON_BUILD_CPACK)
  install(
    TARGETS ${KREON_CLIENT_LIB} ${KREON_SERVER_LIB}
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
    PUBLIC_HEADER DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
  set_target_properties(kreon_client PROPERTIES OUTPUT_NAME ${KREON_CLIENT_LIB})
endif()
