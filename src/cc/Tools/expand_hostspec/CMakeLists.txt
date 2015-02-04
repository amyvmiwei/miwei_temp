#
# Copyright (C) 2007-2015 Hypertable, Inc.
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

# ht_expand_hostspec - Expands a host specification
add_executable(ht_expand_hostspec ht_expand_hostspec.cc)
target_link_libraries(ht_expand_hostspec HyperCommon)

if (NOT HT_COMPONENT_INSTALL)
  install(TARGETS ht_expand_hostspec RUNTIME DESTINATION bin)
endif ()
