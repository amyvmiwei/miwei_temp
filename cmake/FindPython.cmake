# Copyright (C) 2007-2015 Hypertable, Inc.
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

# - Find PYTHON
#  PYTHON_INCLUDE_DIR - where to find Python.h
#  PYTHON_LIBRARIES   - List of libraries when using python-devel
#  PYTHON_FOUND       - True if python-devel was found

exec_program(env ARGS python -V OUTPUT_VARIABLE PYTHON_VERSION_STRING
             RETURN_VALUE PYTHON_RETURN)

if (PYTHON_RETURN STREQUAL "0")
  message(STATUS "Python Shell Version: ${PYTHON_VERSION_STRING}")

  STRING(REGEX REPLACE ".*Python ([0-9]+.[0-9]+).*" "\\1" PYTHON_VERSION "${PYTHON_VERSION_STRING}")

  find_path(PYTHON_INCLUDE_DIR Python.h NO_DEFAULT_PATH PATHS
            ${HT_DEPENDENCY_INCLUDE_DIR}
            /opt/local/include/python${PYTHON_VERSION}
            /opt/local/include/python
            /usr/local/include/python${PYTHON_VERSION}
            /usr/local/include/python
            /usr/include/python${PYTHON_VERSION}
            /usr/include/python
            )

  find_library(PYTHON_LIBRARY python${PYTHON_VERSION} NO_DEFAULT_PATH PATHS
               ${HT_DEPENDENCY_LIB_DIR}
               /opt/local/lib
               /usr/local/lib
               /usr/lib
               /usr/lib/x86_64-linux-gnu
               )

  if (PYTHON_INCLUDE_DIR)
    set(PYTHON_FOUND TRUE)
  endif ()

  if (PYTHON_FOUND)
    message(STATUS "Found Python-devel: ${PYTHON_LIBRARY}")
  else ()
    message(STATUS "Not Found Python-devel: ${PYTHON_LIBRARY}")
  endif ()

else ()
  message(STATUS "Python: not found")
  set(PYTHON_FOUND FALSE)
endif ()


mark_as_advanced(
  PYTHON_LIBRARY
  PYTHON_INCLUDE_DIR
)
