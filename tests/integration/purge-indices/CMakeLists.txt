add_test(Purge-indices-1 env INSTALL_DIR=${INSTALL_DIR}
         ${CMAKE_CURRENT_SOURCE_DIR}/run-delete-cell.sh)
add_test(Purge-indices-2 env INSTALL_DIR=${INSTALL_DIR}
         ${CMAKE_CURRENT_SOURCE_DIR}/run-delete-row.sh)
add_test(Purge-indices-3 env INSTALL_DIR=${INSTALL_DIR}
         ${CMAKE_CURRENT_SOURCE_DIR}/run-delete-cf.sh)
add_test(Purge-indices-4 env INSTALL_DIR=${INSTALL_DIR}
         ${CMAKE_CURRENT_SOURCE_DIR}/run-delete-maxversions.sh)
add_test(Purge-indices-5 env INSTALL_DIR=${INSTALL_DIR}
         ${CMAKE_CURRENT_SOURCE_DIR}/run-delete-ttl.sh)
