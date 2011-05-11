set(SRC_DIR "${HYPERTABLE_SOURCE_DIR}/src/cc/Tools/rsclient/tests")
set(DST_DIR "${HYPERTABLE_BINARY_DIR}/tests/integration/split-merge-loop10")

configure_file(${SRC_DIR}/Test1-data.txt.gz
               ${DST_DIR}/Test1-data.txt.gz COPYONLY)
configure_file(${SRC_DIR}/Test2-data.txt.gz
               ${DST_DIR}/Test2-data.txt.gz COPYONLY)
configure_file(${SRC_DIR}/Test4-data.txt.gz
               ${DST_DIR}/Test4-data.txt.gz COPYONLY)

add_test(RangeServer-split-merge-loop10 env INSTALL_DIR=${INSTALL_DIR}
         env SOURCE_DIR=${HYPERTABLE_SOURCE_DIR}/src/cc/Tools/rsclient/tests
         bash -x ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)
