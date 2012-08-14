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

# test binary
add_executable(issue827_test test.cc)
target_link_libraries(issue827_test Hypertable)

add_test(Defects-issue827-1 env INSTALL_DIR=${INSTALL_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/run.sh)
add_test(Defects-issue827-2 env INSTALL_DIR=${INSTALL_DIR}
        ${HYPERTABLE_BINARY_DIR}/tests/integration/defects/issue827/issue827_test)

