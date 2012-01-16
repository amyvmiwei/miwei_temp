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

# - Find Libuuid 
# Find the libuuid library
#
#  UUID_INCLUDE_DIR - where to find uuid.h
#  UUID_LIBRARIES   - List of libraries when using libuuid
#  UUID_FOUND       - True if libuuid was found

find_path(UUID_INCLUDE_DIR uuid.h NO_DEFAULT_PATH PATHS
  ${HT_DEPENDENCY_INCLUDE_DIR}
  /usr/include
  /usr/include/uuid
  /opt/local/include
  /usr/local/include
)

set(UUID_NAMES ${UUID_NAMES} uuid)
find_library(UUID_LIBRARY NAMES ${UUID_NAMES} NO_DEFAULT_PATH PATHS
    ${HT_DEPENDENCY_LIB_DIR}
    /usr/local/lib
    /opt/local/lib
    /usr/lib
    /lib/x86_64-linux-gnu
    /lib32
    )

if (APPLE AND UUID_INCLUDE_DIR)
  set(UUID_FOUND TRUE)
  set( UUID_LIBRARY "" )
elseif (UUID_INCLUDE_DIR AND UUID_LIBRARY)
  set(UUID_FOUND TRUE)
  set( UUID_LIBRARIES ${UUID_LIBRARY} )
else ()
  set(UUID_FOUND FALSE)
  set( UUID_LIBRARIES )
endif ()

if (UUID_FOUND)
  message(STATUS "Found Uuid: ${UUID_LIBRARY}")
else ()
  message(STATUS "Not Found Uuid: ${UUID_LIBRARY}")
  message(FATAL_ERROR "Could NOT find Uuid library")
endif ()

mark_as_advanced(
  UUID_LIBRARY
  UUID_INCLUDE_DIR
  )
