#
# Copyright (C) 2007-2012 Hypertable, Inc.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 3 of
# the License.
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

set(SRC_DIR "${HYPERTABLE_SOURCE_DIR}/src/cc/Tools/fsclient")
set(DST_DIR "${HYPERTABLE_BINARY_DIR}/src/cc/Tools/fsclient")

set(TEST_DEPENDENCIES ${DST_DIR}/words)

set(fsclient_SRCS
CommandCopyFromLocal.cc
CommandCopyToLocal.cc
CommandLength.cc
CommandMkdirs.cc
CommandRemove.cc
CommandRmdir.cc
CommandShutdown.cc
CommandExists.cc
)

# dfclient lib
add_library(HyperFsCmds ${fsclient_SRCS})
target_link_libraries(HyperFsCmds HyperFsBroker)

# fsclient - command interpreter
add_executable(fsclient fsclient.cc)
target_link_libraries(fsclient HyperFsCmds ${EDITLINE_LIBRARIES})

# fsTest
add_executable(fsTest fsTest.cc fsTestThreadFunction.cc ${TEST_DEPENDENCIES})
target_link_libraries(fsTest HyperFsCmds)

configure_file(${SRC_DIR}/fsTest.golden ${DST_DIR}/fsTest.golden COPYONLY)

add_custom_command(SOURCE ${HYPERTABLE_SOURCE_DIR}/tests/data/words.gz
    COMMAND gzip ARGS -d < ${HYPERTABLE_SOURCE_DIR}/tests/data/words.gz
                         > ${DST_DIR}/words
    OUTPUT ${DST_DIR}/words)

set(ADDITIONAL_MAKE_CLEAN_FILES ${DST_DIR}/words)

add_test(HyperFsBroker fsTest)

if (NOT HT_COMPONENT_INSTALL)
  install(TARGETS HyperFsCmds fsclient
          RUNTIME DESTINATION bin
          LIBRARY DESTINATION lib
          ARCHIVE DESTINATION lib)
endif ()
