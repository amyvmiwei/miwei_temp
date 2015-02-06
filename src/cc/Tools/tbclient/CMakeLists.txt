#
# Copyright (C) 2007-2015 Hypertable, Inc.
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

set(SRC_DIR "${HYPERTABLE_SOURCE_DIR}/src/cc/Tools/tbclient")
set(DST_DIR "${HYPERTABLE_BINARY_DIR}/src/cc/Tools/tbclient")

set(tbclient_SRCS
CommandInterpreter.cc
tbclient.cc
)

# ht_tbclient - command interpreter
add_executable(ht_tbclient ${tbclient_SRCS})
target_link_libraries(ht_tbclient HyperCommon HyperThriftConfig HyperThrift ${EDITLINE_LIBRARIES})

if (NOT HT_COMPONENT_INSTALL)
  install(TARGETS ht_tbclient
          RUNTIME DESTINATION bin
          LIBRARY DESTINATION lib
          ARCHIVE DESTINATION lib)
endif ()
