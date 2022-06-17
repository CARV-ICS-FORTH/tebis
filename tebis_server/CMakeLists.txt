set_source_files_properties(../utilities/spin_loop.c PROPERTIES COMPILE_FLAGS
                                                                "-O0")
add_definitions(-DtebisR)

add_library(
  tebis_client
  messages.c
  ../tebis_rdma_client/client_utils.c
  ../tebis_rdma_client/msg_factory.c
  ../tebis_rdma_client/tebis_rdma_client.c
  ../tebis_server/globals.c
  ../common/common.c
  zk_utils.c
  djb2.c)
add_dependencies(tebis_client cjson)

target_link_libraries(tebis_client ${Zookeeper_LIBRARY})

add_executable(
  tebis_server
  server.c
  master.c
  metadata_server.c
  build_index/build_index.c
  send_index/send_index.c
  send_index/send_index_callbacks.c
  send_index/send_index_uuid_checker/send_index_uuid_checker.c
  send_index/send_index_segment_iterator.c
  send_index/send_index_rewriter.c
  ../common/common.c
  rdma_buffer_iterator/rdma_buffer_iterator.c
  region_server.c
  remote_compaction.c
  deserialize_index.c
  zk_utils.c
  djb2.c
  globals.c
  messages.c
  server_communication.c
  ../tebis_rdma/rdma.c
  ../tebis_rdma_client/client_utils.c
  ../tebis_rdma_client/msg_factory.c
  stats.c)

add_dependencies(tebis_server cjson)
target_link_libraries(
  tebis_server
  tebis_rdma
  parallax
  log
  ${Zookeeper_LIBRARY}
  ${cjson_LIBRARY}
  pthread
  numa
  ibverbs
  rdmacm
  rt
  m)

add_executable(create_region create_region.c zk_utils.c
                             ../utilities/spin_loop.c)

target_link_libraries(create_region log ${Zookeeper_LIBRARY})
target_link_options(create_region PUBLIC -gdwarf-4 -pthread)

add_executable(create_server_node create_server_node.c zk_utils.c
                                  ../utilities/spin_loop.c)
target_link_libraries(create_server_node log ${Zookeeper_LIBRARY})
target_link_options(create_server_node PUBLIC -gdwarf-4 -pthread)

set_target_properties(
  tebis_client PROPERTIES PUBLIC_HEADER
                          "../tebis_rdma_client/tebis_rdma_client.h")

set(tebis_CLIENT_LIB "tebis_client")
set(tebis_SERVER_LIB "tebis_server")
if(tebis_BUILD_CPACK)
  install(
    TARGETS ${tebis_CLIENT_LIB} ${tebis_SERVER_LIB}
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
    PUBLIC_HEADER DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
  set_target_properties(tebis_client PROPERTIES OUTPUT_NAME ${tebis_CLIENT_LIB})
endif()

if(RUN_IWYU STREQUAL "ON")
  find_program(iwyu_path NAMES include-what-you-use iwyu iwyu-tool)
  if(iwyu_path)
    set_property(TARGET ${tebis_CLIENT_LIB} PROPERTY C_INCLUDE_WHAT_YOU_USE
                                                     ${iwyu_path})
    set_property(TARGET ${tebis_SERVER_LIB} PROPERTY C_INCLUDE_WHAT_YOU_USE
                                                     ${iwyu_path})
    set_property(TARGET create_region PROPERTY C_INCLUDE_WHAT_YOU_USE
                                               ${iwyu_path})
    set_property(TARGET create_server_node PROPERTY C_INCLUDE_WHAT_YOU_USE
                                                    ${iwyu_path})
  else()
    message("Could not find the program include-what-you-use")
  endif()
endif()
