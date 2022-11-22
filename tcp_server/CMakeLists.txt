# File: tcp_server/CMakeLists.txt Author: Orestis Chiotakis
# <orchiot@ics.forth.gr>

set(tcp_server_EXEC "tcp-server")

add_executable(${tcp_server_EXEC} server.c tcp_server.c)

target_link_libraries(${tcp_server_EXEC} log)

set_target_properties(
	${tcp_server_EXEC}
	PROPERTIES LINK_FLAGS
		"-pthread"
)

install(
  TARGETS ${tcp_server_EXEC}
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
  PUBLIC_HEADER DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")

set_target_properties(${tcp_server_EXEC} PROPERTIES OUTPUT_NAME
                                                    ${tcp_server_EXEC})
