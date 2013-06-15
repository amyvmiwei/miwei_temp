set(SRC_DIR "${HYPERTABLE_SOURCE_DIR}/tests/integration/no_log")

add_test(insert-no-log env INSTALL_DIR=${INSTALL_DIR}
         ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)
