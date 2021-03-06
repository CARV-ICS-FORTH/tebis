set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(KREON_RDMA_LIB "kreon_rdma")

add_library(
  kreon_rdma
  memory_region_pool.c
  rdma.c
  ../kreon_rdma_client/kreon_rdma_client.c
  ../utilities/queue.c
  ../utilities/simple_concurrent_list.c
  ../utilities/spin_loop.c
  ../utilities/circular_buffer.c
  ../utilities/list.c)

target_link_libraries(kreon_rdma ${Zookeeper_LIBRARY})

if(KREON_BUILD_CPACK)
  install(
    TARGETS ${KREON_RDMA_LIB}
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
    PUBLIC_HEADER DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
  set_target_properties(${LIB} PROPERTIES OUTPUT_NAME ${KREON_RDMA_LIB})
endif()

if(RUN_IWYU STREQUAL "ON")
  find_program(iwyu_path NAMES include-what-you-use iwyu iwyu-tool)
  if(iwyu_path)
    set_property(TARGET ${KREON_RDMA_LIB} PROPERTY C_INCLUDE_WHAT_YOU_USE
                                                   ${iwyu_path})
  else()
    message("Could not find the program include-what-you-use")
  endif()
endif()
