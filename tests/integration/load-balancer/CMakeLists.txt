add_test(Balancing-load env INSTALL_DIR=${INSTALL_DIR}
         TEST_BIN_DIR=${HYPERTABLE_BINARY_DIR}/src/cc/Hypertable/Master/
         ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)
