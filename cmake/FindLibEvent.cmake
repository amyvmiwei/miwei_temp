# Copyright (C) 2007-2012 Hypertable, Inc.
#
# This file is part of Hypertable.
#
# Hypertable is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 3
# of the License, or any later version.
#
# Hypertable is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Hypertable. If not, see <http://www.gnu.org/licenses/>
#

# - Find LibEvent (a cross platform RPC lib/tool)
# This module defines
#  LibEvent_INCLUDE_DIR, where to find LibEvent headers
#  LibEvent_LIBS, LibEvent libraries
#  LibEvent_FOUND, If false, do not try to use ant

find_path(LibEvent_INCLUDE_DIR event.h PATHS
    /usr/local/include
    /opt/local/include
  )

set(LibEvent_LIB_PATHS /usr/local/lib /opt/local/lib)
find_library(LibEvent_LIB NAMES event PATHS ${LibEvent_LIB_PATHS})

if (LibEvent_LIB AND LibEvent_INCLUDE_DIR)
  set(LibEvent_FOUND TRUE)
  set(LibEvent_LIBS ${LibEvent_LIB})
else ()
  set(LibEvent_FOUND FALSE)
endif ()

if (LibEvent_FOUND)
  if (NOT LibEvent_FIND_QUIETLY)
    message(STATUS "Found libevent: ${LibEvent_LIBS}")
  endif ()
else ()
  message(STATUS "libevent NOT found.")
endif ()

mark_as_advanced(
    LibEvent_LIB
    LibEvent_INCLUDE_DIR
  )
