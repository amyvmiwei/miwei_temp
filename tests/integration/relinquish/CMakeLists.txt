add_test(Relinquish-move-range-exception env DATA_SIZE=200000
         INSTALL_DIR=${INSTALL_DIR}
         PYTHONPATH=${HYPERTABLE_SOURCE_DIR}/src/py/ThriftClient:${HYPERTABLE_SOURCE_DIR}/src/py/ThriftClient/gen-py
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/move-range-exception.sh)
