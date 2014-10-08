#
# Copyright (C) 2007-2014 Hypertable, Inc.
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

set(SRC_DIR "${HYPERTABLE_SOURCE_DIR}/src/cc/Tools/rsclient")
set(DST_DIR "${HYPERTABLE_BINARY_DIR}/src/cc/Tools/rsclient")

set(TEST_DEPENDENCIES ${DST_DIR}/Test1-data.txt ${DST_DIR}/Test2-data.txt
    ${DST_DIR}/Test4-data.txt)

set(ht_rsclient_SRCS
ht_rsclient.cc
RangeServerCommandInterpreter.cc
TableInfo.cc
)

# ht_rsclient - Range Server client command interpreter
add_executable(ht_rsclient ${ht_rsclient_SRCS} ${TEST_DEPENDENCIES})
target_link_libraries(ht_rsclient Hypertable)

add_custom_command(SOURCE ${SRC_DIR}/tests/Test1-data.txt.gz
                   COMMAND cp ARGS ${SRC_DIR}/tests/Test1-data.txt.gz ${DST_DIR}
                   COMMAND gunzip ARGS -f ${DST_DIR}/Test1-data.txt.gz
                   OUTPUT ${DST_DIR}/Test1-data.txt)
add_custom_command(SOURCE ${SRC_DIR}/tests/Test2-data.txt.gz
                   COMMAND cp ARGS ${SRC_DIR}/tests/Test2-data.txt.gz ${DST_DIR}
                   COMMAND gunzip ARGS -f ${DST_DIR}/Test2-data.txt.gz
                   OUTPUT ${DST_DIR}/Test2-data.txt)
add_custom_command(SOURCE ${SRC_DIR}/tests/Test4-data.txt.gz
                   COMMAND cp ARGS ${SRC_DIR}/tests/Test4-data.txt.gz ${DST_DIR}
                   COMMAND gunzip ARGS -f ${DST_DIR}/Test4-data.txt.gz
                   OUTPUT ${DST_DIR}/Test4-data.txt)

add_test(RangeServer env INSTALL_DIR=${INSTALL_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/tests/run.sh)

if (NOT HT_COMPONENT_INSTALL)
  install(TARGETS ht_rsclient RUNTIME DESTINATION bin)
endif ()
