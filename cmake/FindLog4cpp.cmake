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

# - Find Log4cpp
# Find the native Log4cpp includes and library
#
#  Log4cpp_INCLUDE_DIR - where to find Log4cpp.h, etc.
#  Log4cpp_LIBRARIES   - List of libraries when using Log4cpp.
#  Log4cpp_FOUND       - True if Log4cpp found.

if (Log4cpp_INCLUDE_DIR)
  # Already in cache, be silent
  set(Log4cpp_FIND_QUIETLY TRUE)
endif ()

find_path(Log4cpp_INCLUDE_DIR log4cpp/Category.hh NO_DEFAULT_PATH PATHS
  ${HT_DEPENDENCY_INCLUDE_DIR}
  /opt/local/include
  /usr/local/include
  /usr/include
)

set(Log4cpp_NAMES log4cpp)
find_library(Log4cpp_LIBRARY NO_DEFAULT_PATH
  NAMES ${Log4cpp_NAMES}
  PATHS ${HT_DEPENDENCY_LIB_DIR} /usr/lib /usr/local/lib /opt/local/lib
)

if (Log4cpp_INCLUDE_DIR AND Log4cpp_LIBRARY)
  set(Log4cpp_FOUND TRUE)
  set( Log4cpp_LIBRARIES ${Log4cpp_LIBRARY} )
else ()
  set(Log4cpp_FOUND FALSE)
  set( Log4cpp_LIBRARIES )
endif ()

if (Log4cpp_FOUND)
  if (NOT Log4cpp_FIND_QUIETLY)
    message(STATUS "Found Log4cpp: ${Log4cpp_LIBRARIES}")
  endif ()
else ()
  if (Log4cpp_FIND_REQUIRED)
    message(STATUS "Looked for Log4cpp libraries named ${Log4cppS_NAMES}.")
    message(FATAL_ERROR "Could NOT find Log4cpp library")
  endif ()
endif ()

mark_as_advanced(
  Log4cpp_LIBRARY
  Log4cpp_INCLUDE_DIR
  )
