
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/thrift-table-refresh.golden
               ${CMAKE_CURRENT_BINARY_DIR}/thrift-table-refresh.golden)

add_test(Thrift-table-refresh env INSTALL_DIR=${INSTALL_DIR}
    PYTHONPATH=${HYPERTABLE_SOURCE_DIR}/src/py/ThriftClient:${HYPERTABLE_SOURCE_DIR}/src/py/ThriftClient/gen-py
         ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)
