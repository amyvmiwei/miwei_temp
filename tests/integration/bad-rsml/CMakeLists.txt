add_test(Bad-RSML-Master-recover env TEST=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run-master-recover.sh)

add_test(Bad-RSML-RangeServer-recover env TEST=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run-rangeserver-recover.sh)
