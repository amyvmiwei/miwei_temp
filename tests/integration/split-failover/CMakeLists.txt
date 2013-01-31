add_test(split-failover-1 env TEST=1 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(split-failover-2 env TEST=2 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(split-failover-3 env TEST=3 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(split-failover-4 env TEST=4 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(split-failover-5 env TEST=5 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(split-failover-6 env TEST=6 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(split-failover-7 env TEST=7 DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)
