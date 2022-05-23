# File: kreon/CMakeLists.txt Author: Michalis Vardoulakis <mvard@ics.forth.gr>

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set_source_files_properties(test_streaming_btree.c PROPERTIES COMPILE_FLAGS
                                                              "-O3")

set_source_files_properties(test_circular_buffer.c PROPERTIES COMPILE_FLAGS
                                                              "-O3")

set_source_files_properties(test_scans.c PROPERTIES COMPILE_FLAGS "-O3")
set_source_files_properties(test_dirty_scans.c PROPERTIES COMPILE_FLAGS "-O3")

set_source_files_properties(test_krc_scans.c PROPERTIES COMPILE_FLAGS "-O3")

set(CMAKE_EXE_LINKER_FLAGS "-Wl,-no-as-needed -lm -lrt -pthread -lnuma")

# add_executable(streaming_btree_test test_streaming_btree.c)
# target_link_libraries(streaming_btree_test runtime)

add_executable(test_allocator test_allocator.c)
target_link_libraries(test_allocator ${Zookeeper_LIBRARY} kreon)

add_executable(test_scans test_scans.c)
target_link_libraries(test_scans kreon)

add_executable(test_dirty_scans test_dirty_scans.c)
target_link_libraries(test_dirty_scans ${Zookeeper_LIBRARY} kreon)

add_executable(test_circular_buffer test_circular_buffer.c
                                    ../utilities/circular_buffer.c)

target_link_libraries(test_circular_buffer log ${Zookeeper_LIBRARY})

# Targets with dependencies to Zookeeper should be added in this if block
if(Zookeeper_LIBRARY)
  add_executable(test_krc_api test_krc_api.c)
  target_link_libraries(
    test_krc_api
    kreon_client
    kreon_rdma
    kreonr
    log
    ${Zookeeper_LIBRARY}
    ${cjson_LIBRARY})
  add_dependencies(test_krc_api cjson)
  set_target_properties(
    test_krc_api
    PROPERTIES LINK_FLAGS
               "-Wl,-no-as-needed -pthread -lm -lnuma -lrdmacm -libverbs"
               COMPILE_DEFINITIONS "KREON_DISTRIBUTED")

  if(RUN_IWYU STREQUAL "ON")
    find_program(iwyu_path NAMES include-what-you-use iwyu iwyu-tool)
    if(iwyu_path)
      set_property(TARGET test_krc_api PROPERTY C_INCLUDE_WHAT_YOU_USE
                                                ${iwyu_path})
    else()
      message("Could not find the program include-what-you-use")
    endif()
  endif()
endif()
