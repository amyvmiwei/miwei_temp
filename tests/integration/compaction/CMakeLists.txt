add_test(Compaction-exception-manual-1 env TEST=1 INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(Compaction-exception-manual-2 env TEST=2 INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(Compaction-exception-split-1 env TEST=3 INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(Compaction-exception-split-2 env TEST=4 INSTALL_DIR=${INSTALL_DIR}
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(Compaction-exception-relinquish-1 env TEST=5 INSTALL_DIR=${INSTALL_DIR}
         PYTHONPATH=${HYPERTABLE_SOURCE_DIR}/src/py/ThriftClient:${HYPERTABLE_SOURCE_DIR}/src/py/ThriftClient/gen-py
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)

add_test(Compaction-exception-relinquish-2 env TEST=6 INSTALL_DIR=${INSTALL_DIR}
         PYTHONPATH=${HYPERTABLE_SOURCE_DIR}/src/py/ThriftClient:${HYPERTABLE_SOURCE_DIR}/src/py/ThriftClient/gen-py
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)
