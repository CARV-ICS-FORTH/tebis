# File: tcp_server/CMakeLists.txt Author: Orestis Chiotakis
# <orchiot@ics.forth.gr>

set(tcp_server_EXEC "tcp-server")
set(tiresias_tcp_EXEC "tiresias-tcp")
set(BDB_PATH "/user/lib64/libdb-5.3.so")

add_executable(${tcp_server_EXEC} server.c plog.c server_handle.c)
target_link_libraries(${tcp_server_EXEC} log m pthread rt parallax)

install(
  TARGETS ${tcp_server_EXEC}
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
  PUBLIC_HEADER DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")

set_target_properties(${tcp_server_EXEC} PROPERTIES OUTPUT_NAME
                                                    ${tcp_server_EXEC})

# find_library(BDB_LIBRARY NAMES libdb.so)

message("Building tiresias-tcp")
add_executable(${tiresias_tcp_EXEC} tiresias.c arg_parser.c)
target_link_libraries(${tiresias_tcp_EXEC} ${BDB_PATH})
