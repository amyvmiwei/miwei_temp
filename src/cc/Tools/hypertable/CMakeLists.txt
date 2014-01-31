#
# Copyright (C) 2007-2012 Hypertable, Inc.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 3
# of the License, or any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301, USA.
#

set(SRC_DIR "${HYPERTABLE_SOURCE_DIR}/src/cc/Tools/hypertable")
set(DST_DIR "${HYPERTABLE_BINARY_DIR}/src/cc/Tools/hypertable")

set(hypertable_SRCS
hypertable.cc
)

# hypertable - command interpreter
add_executable(hypertable ${hypertable_SRCS})
target_link_libraries(hypertable Hypertable)

# hypertable_test
add_executable(hypertable_test test/hypertable_test.cc ${TEST_DEPENDENCIES})
target_link_libraries(hypertable_test HyperComm)


configure_file(${HYPERTABLE_SOURCE_DIR}/conf/hypertable.cfg
               ${DST_DIR}/hypertable.cfg)
configure_file(${SRC_DIR}/test/offset_test.golden
               ${DST_DIR}/offset_test.golden)
configure_file(${SRC_DIR}/test/offset_test.hql
               ${DST_DIR}/offset_test.hql)
configure_file(${SRC_DIR}/test/timeorder_test.golden
               ${DST_DIR}/timeorder_test.golden)
configure_file(${SRC_DIR}/test/timeorder_test.hql
               ${DST_DIR}/timeorder_test.hql)
configure_file(${SRC_DIR}/test/indices_test.golden
               ${DST_DIR}/indices_test.golden)
configure_file(${SRC_DIR}/test/indices_test.hql
               ${DST_DIR}/indices_test.hql)
configure_file(${SRC_DIR}/test/indices_test_products.tsv
               ${DST_DIR}/indices_test_products.tsv COPYONLY)
configure_file(${SRC_DIR}/test/hypertable_test.golden
               ${DST_DIR}/hypertable_test.golden)
configure_file(${SRC_DIR}/test/hypertable_select_gz_test.golden
               ${DST_DIR}/hypertable_select_gz_test.golden)
configure_file(${SRC_DIR}/test/hypertable_test.hql
               ${DST_DIR}/hypertable_test.hql)
configure_file(${SRC_DIR}/test/hypertable_test.tsv
               ${DST_DIR}/hypertable_test.tsv)
configure_file(${SRC_DIR}/test/hypertable_unknown_cf.tsv
               ${DST_DIR}/hypertable_unknown_cf.tsv)
configure_file(${SRC_DIR}/test/hypertable_ts.tsv
               ${DST_DIR}/hypertable_ts.tsv)
configure_file(${SRC_DIR}/test/foobar.tsv
               ${DST_DIR}/foobar.tsv)
configure_file(${SRC_DIR}/test/single_cell_test.tsv
               ${DST_DIR}/single_cell_test.tsv)

add_test(Hypertable-shell env INSTALL_DIR=${INSTALL_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/test/hypertable_test.sh hypertable_test)

# hypertable_ldi_select_test
add_executable(hypertable_ldi_select_test test/hypertable_ldi_select_test.cc ${TEST_DEPENDENCIES})
target_link_libraries(hypertable_ldi_select_test HyperDfsBroker HyperComm)

configure_file(${SRC_DIR}/test/hypertable_ldi_stdin_test_load.hql
               ${DST_DIR}/hypertable_ldi_stdin_test_load.hql)
configure_file(${SRC_DIR}/test/hypertable_ldi_stdin_test_select.hql
               ${DST_DIR}/hypertable_ldi_stdin_test_select.hql)
configure_file(${SRC_DIR}/test/hypertable_ldi_select_test.golden
               ${DST_DIR}/hypertable_ldi_select_test.golden)
configure_file(${SRC_DIR}/test/hypertable_test.tsv.gz 
               ${DST_DIR}/hypertable_test.tsv.gz COPYONLY)
configure_file(${SRC_DIR}/test/hypertable_escape_test.tsv
               ${DST_DIR}/hypertable_escape_test.tsv COPYONLY)
configure_file(${SRC_DIR}/test/hypertable_escape_test.golden
               ${DST_DIR}/hypertable_escape_test.golden COPYONLY)
add_test(Hypertable-shell-ldi-select hypertable_ldi_select_test ${INSTALL_DIR})

# field_separator_test
configure_file(${SRC_DIR}/test/field_separator_test.tsv
               ${DST_DIR}/field_separator_test.tsv COPYONLY)
add_test(Alternate-field-separator env INSTALL_DIR=${INSTALL_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/test/field_separator_test.sh)

# delete_test
configure_file(${SRC_DIR}/test/delete_test.tsv
               ${DST_DIR}/delete_test.tsv COPYONLY)
add_test(Delete-tsv-test env INSTALL_DIR=${INSTALL_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/test/delete_test.sh)

if (NOT HT_COMPONENT_INSTALL)
  install(TARGETS hypertable RUNTIME DESTINATION bin)
endif ()
