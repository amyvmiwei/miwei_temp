configure_file(${CMAKE_CURRENT_SOURCE_DIR}/hyperspace_dump_test1.in
               ${CMAKE_CURRENT_BINARY_DIR}/hyperspace_dump_test1.in)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/hyperspace_dump_test1.golden
               ${CMAKE_CURRENT_BINARY_DIR}/hyperspace_dump_test1.golden)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/hyperspace_dump_test2.in
               ${CMAKE_CURRENT_BINARY_DIR}/hyperspace_dump_test2.in)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/hyperspace_dump_test2.golden
               ${CMAKE_CURRENT_BINARY_DIR}/hyperspace_dump_test2.golden)

add_test(Hyperspace-dump env INSTALL_DIR=${INSTALL_DIR}
         ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)
