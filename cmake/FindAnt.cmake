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

# - Find Ant (a java build tool)
# This module defines
#  ANT_VERSION version string of ant if found
#  ANT_FOUND, If false, do not try to use ant

exec_program(env ARGS ant -version OUTPUT_VARIABLE ANT_VERSION
             RETURN_VALUE ANT_RETURN)

if (ANT_RETURN STREQUAL "0")
  set(ANT_FOUND TRUE)
  if (NOT ANT_FIND_QUIETLY)
    message(STATUS "Found Ant: ${ANT_VERSION}")
  endif ()
else ()
  message(STATUS "Ant: not found")
  set(ANT_FOUND FALSE)
  set(SKIP_JAVA_BUILD TRUE)
endif ()

exec_program(env ARGS javac -version OUTPUT_VARIABLE JAVAC_OUT
             RETURN_VALUE JAVAC_RETURN)

if (JAVAC_RETURN STREQUAL "0")
  message(STATUS "    Javac: ${JAVAC_OUT}")
  string(REGEX MATCH "1\\.6\\..*" JAVAC_VERSION ${JAVAC_OUT})

  if (NOT JAVAC_VERSION)
    message(STATUS "    Expected JDK 1.6.*. Skipping Java build")
    set(SKIP_JAVA_BUILD TRUE)
  endif ()
else ()
  message(STATUS "    Javac: not found")
  set(SKIP_JAVA_BUILD TRUE)
endif ()
