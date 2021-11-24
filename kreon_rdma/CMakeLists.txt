# File: TuRDMA/CMakeLists.txt Author: Michalis Vardoulakis <mvard@ics.forth.gr>

# add_library(turdma STATIC memory_regions.c tu_rdma.c
# ../kreon/allocator/list.c)

if("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
  set(CMAKE_C_FLAGS
      "-Wstrict-prototypes -Wall -Wextra -std=gnu11 -fPIC ${CMAKE_C_FLAGS}"
  )# -Werror
  set(CMAKE_C_FLAGS_DEBUG "-ggdb3 -Og ${CMAKE_C_FLAGS_DEBUG}")
endif()

if(CMAKE_BUILD_TYPE EQUAL "Release")
  set(CMAKE_C_FLAGS_RELEASE
      "-flto -fwhopr -funit-at-a-time -fprefetch-loop-arrays -fpeel-loops -O2 -finline-functions -DNDEBUG ${CMAKE_C_FLAGS_RELEASE}"
  ) # -Werror
endif()

set(CMAKE_SHARED_LINKER_FLAGS
    "-Wl,-no-as-needed -lm -pthread -lnuma -libverbs -lrdmacm")

add_library(
  kreon_rdma STATIC
  memory_region_pool.c
  rdma.c
  ../kreon_rdma_client/kreon_rdma_client.c
  ../utilities/queue.c
  ../utilities/simple_concurrent_list.c
  ../utilities/spin_loop.c
  ../utilities/circular_buffer.c
  ../utilities/list.c)

add_library(
  kreon_rdma-shared SHARED
  memory_region_pool.c
  rdma.c
  ../kreon_rdma_client/kreon_rdma_client.c
  ../utilities/queue.c
  ../utilities/simple_concurrent_list.c
  ../utilities/spin_loop.c
  ../utilities/circular_buffer.c
  ../utilities/list.c)

# add_executable(test_rdma test_rdma.c) set_target_properties(test_rdma
# PROPERTIES LINK_FLAGS "-libverbs -pthread -D_GNU_SOURCE" )
# target_link_libraries(test_rdma turdma)
if(KREON_BUILD_CPACK)
  install(TARGETS kreon_rdma-shared
          LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}")
  set_target_properties(kreon_rdma-shared PROPERTIES OUTPUT_NAME kreon_rdma)
endif()
