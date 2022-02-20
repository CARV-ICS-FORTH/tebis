# File: kreon/CMakeLists.txt Author: Michalis Vardoulakis <mvard@ics.forth.gr>

include_directories(${CMAKE_CURRENT_BINARY_DIR})
set(KREON_LIB "kreon")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

file(
  GLOB_RECURSE KREON_C_SOURCE_FILES ${CMAKE_SOURCE_DIR}/kreon_lib/allocator/*.c
  ${CMAKE_SOURCE_DIR}/kreon_lib/btree/*.c
  ${CMAKE_SOURCE_DIR}/kreon_lib/scanner/*.c)

# list(REMOVE_ITEM KREON_C_SOURCE_FILES
list(APPEND KREON_C_SOURCE_FILES "${CMAKE_SOURCE_DIR}/utilities/spin_loop.c")
list(APPEND KREON_C_SOURCE_FILES "${CMAKE_SOURCE_DIR}/utilities/list.c")

# Kreon with replication enabled
add_library(
  kreonr
  allocator/allocator.c
  allocator/persistent_operations.c
  btree/segment_allocator.c
  btree/btree.c
  btree/compaction_daemon.c
  btree/gc.c
  btree/asyncio.c
  scanner/stack.c
  scanner/scanner.c
  scanner/min_max_heap.c
  ../utilities/spin_loop.c
  ../utilities/list.c)

set_target_properties(kreonr PROPERTIES COMPILE_DEFINITIONS "KREONR")

# Kreon with replication disabled
add_library(${KREON_LIB} ${KREON_C_SOURCE_FILES})
set_property(TARGET ${KREON_LIB} PROPERTY POSITION_INDEPENDENT_CODE ON)
target_link_libraries(${KREON_LIB} log libbloom rt)
target_link_libraries(kreonr log libbloom rt)

set_source_files_properties(allocator/spin_loop.c PROPERTIES COMPILE_FLAGS
                                                             "-O0")
# mkfs for kreon volumes
add_executable(
  mkfs.kreon
  allocator/mkfs_Eutropia.c
  allocator/allocator.c
  allocator/persistent_operations.c
  btree/segment_allocator.c
  ../utilities/spin_loop.c
  ../utilities/list.c
  btree/asyncio.c)

target_link_options(mkfs.kreon PUBLIC -lrt -pthread -lm)
target_link_libraries(mkfs.kreon log libbloom)

if(KREON_BUILD_CPACK)
  install(
    TARGETS ${KREON_LIB} mkfs.kreon kreonr
    ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
    RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}")
  set_target_properties(${KREON_LIB} PROPERTIES OUTPUT_NAME kreon)
  set_target_properties(kreonr PROPERTIES OUTPUT_NAME kreonr)
endif()
