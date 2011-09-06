# -*- cmake -*-

# - Find RRDtool 
# Find the RRDtool includes and library
# This module defines
#  RRD_INCLUDE_DIR, where to find rrd.h, etc.
#  RRD_LIBRARIES, the libraries needed to use RRDtool.
#  RRD_FOUND, If false, do not try to use RRDtool.
#  also defined, RD_LIBRARY, where to find the RRDtool library.

find_path(RRD_INCLUDE_DIR rrd.h NO_DEFAULT_PATH PATHS
    ${HT_DEPENDENCY_INCLUDE_DIR}
    /opt/local/include
    /usr/local/include
    /usr/include
    )

set(RRD_NAMES ${RRD_NAMES} rrd)
find_library(RRD_LIBRARY NAMES ${RRD_NAMES} NO_DEFAULT_PATH PATHS
    ${HT_DEPENDENCY_LIB_DIR}
    /opt/local/lib
    /usr/local/lib
    /usr/lib
    /usr/lib64
    )

find_library(FREETYPE_LIBRARY freetype PATHS
    ${HT_DEPENDENCY_LIB_DIR}
    /opt/local/lib
    /usr/local/lib
    /usr/lib/i386-linux-gnu
    /usr/lib
    /usr/lib64
    )

set(PNG_NAMES ${PNG_NAMES} png12 png14)
find_library(PNG_LIBRARY NAMES ${PNG_NAMES} NO_DEFAULT_PATH PATHS
    ${HT_DEPENDENCY_LIB_DIR}
    /opt/local/lib
    /usr/local/lib
    /usr/lib/i386-linux-gnu
    /usr/lib
    /usr/lib64
    )

find_library(PANGOCAIRO_LIBRARY NAMES pangocairo NO_DEFAULT_PATH PATHS
    ${HT_DEPENDENCY_LIB_DIR}
    /opt/local/lib
    /usr/local/lib
    /usr/lib/i386-linux-gnu
    /usr/lib
    /usr/lib64
    )

find_library(PANGO_LIBRARY NAMES pango NO_DEFAULT_PATH PATHS
    ${HT_DEPENDENCY_LIB_DIR}
    /opt/local/lib
    /usr/local/lib
    /usr/lib/i386-linux-gnu
    /usr/lib
    /usr/lib64
    /lib
    /lib64
    )

find_library(CAIRO_LIBRARY NAMES cairo NO_DEFAULT_PATH PATHS
    ${HT_DEPENDENCY_LIB_DIR}
    /opt/local/lib
    /usr/local/lib
    /usr/lib
    /usr/lib64
    /lib
    /lib64
    )

find_library(PANGOFT2_LIBRARY NAMES pangoft2 NO_DEFAULT_PATH PATHS
    ${HT_DEPENDENCY_LIB_DIR}
    /opt/local/lib
    /usr/local/lib
    /usr/lib/i386-linux-gnu
    /usr/lib
    /usr/lib64
    /lib
    /lib64
    )


find_library(GOBJECT_LIBRARY NAMES gobject NO_DEFAULT_PATH PATHS
    ${HT_DEPENDENCY_LIB_DIR}
    /opt/local/lib
    /usr/local/lib
    /usr/lib/i386-linux-gnu
    /usr/lib
    /usr/lib64
    /lib
    /lib64
    )

find_library(GMODULE_LIBRARY NAMES gmodule NO_DEFAULT_PATH PATHS
    ${HT_DEPENDENCY_LIB_DIR}
    /opt/local/lib
    /usr/local/lib
    /usr/lib/i386-linux-gnu
    /usr/lib
    /usr/lib64
    /lib
    /lib64
    )

find_library(GLIB_LIBRARY NAMES glib NO_DEFAULT_PATH PATHS
    ${HT_DEPENDENCY_LIB_DIR}
    /opt/local/lib
    /usr/local/lib
    /usr/lib/i386-linux-gnu
    /usr/lib
    /usr/lib64
    /lib
    /lib64
    )

find_library(ART_LGPL_2_LIBRARY NAMES art_lgpl_2 NO_DEFAULT_PATH PATHS
    ${HT_DEPENDENCY_LIB_DIR}
    /opt/local/lib
    /usr/local/lib
    /usr/lib/i386-linux-gnu
    /usr/lib
    /usr/lib64
    )

if (APPLE)

  find_library(PIXMAN_LIBRARY NAMES pixman NO_DEFAULT_PATH PATHS
    ${HT_DEPENDENCY_LIB_DIR}
    /opt/local/lib
    /usr/local/lib
    /usr/lib
    /usr/lib64
    )

find_library(GTHREAD_LIBRARY NAMES gthread NO_DEFAULT_PATH PATHS
    ${HT_DEPENDENCY_LIB_DIR}
    /opt/local/lib
    /usr/local/lib
    /usr/lib/i386-linux-gnu
    /usr/lib
    /usr/lib64
    )

  message(STATUS "${RRD_LIBRARY} ${RRD_INCLUDE_DIR} ${FREETYPE_LIBRARY} ${PNG_LIBRARY} ${PANGOCAIRO_LIBRARY} ${PANGO_LIBRARY} ${PANGOFT2_LIBRARY} ${CAIRO_LIBRARY} ${PIXMAN_LIBRARY} ${GOBJECT_LIBRARY} ${GMODULE_LIBRARY} ${GTHREAD_LIBRARY} ${GLIB_LIBRARY} ${ART_LGPL_2_LIBRARY}")
  
  if (RRD_LIBRARY AND RRD_INCLUDE_DIR AND FREETYPE_LIBRARY AND PNG_LIBRARY AND PANGOCAIRO_LIBRARY AND PANGO_LIBRARY AND PANGOFT2_LIBRARY AND CAIRO_LIBRARY AND PIXMAN_LIBRARY AND GOBJECT_LIBRARY AND GMODULE_LIBRARY AND GTHREAD_LIBRARY AND GLIB_LIBRARY AND ART_LGPL_2_LIBRARY)
    set(RRD_LIBRARIES ${RRD_LIBRARY} ${FREETYPE_LIBRARY} ${PNG_LIBRARY} ${PANGOCAIRO_LIBRARY} ${PANGO_LIBRARY} ${PANGOFT2_LIBRARY} ${CAIRO_LIBRARY} ${PIXMAN_LIBRARY} ${GOBJECT_LIBRARY} ${GMODULE_LIBRARY} ${GTHREAD_LIBRARY} ${GLIB_LIBRARY} ${ART_LGPL_2_LIBRARY})
    set(RRD_FOUND TRUE)
    message(STATUS "Found RRDtool: ${RRD_LIBRARIES}")
  else ()
    set(RRD_FOUND FALSE)
  endif ()

else ()

  message(STATUS "${RRD_LIBRARY} ${RRD_INCLUDE_DIR} ${FREETYPE_LIBRARY} ${PNG_LIBRARY} ${PANGOCAIRO_LIBRARY} ${PANGO_LIBRARY} ${PANGOFT2_LIBRARY} ${CAIRO_LIBRARY} ${GOBJECT_LIBRARY} ${GMODULE_LIBRARY} ${GLIB_LIBRARY} ${ART_LGPL_2_LIBRARY}")
  
  if (RRD_LIBRARY AND RRD_INCLUDE_DIR AND FREETYPE_LIBRARY AND PNG_LIBRARY AND PANGOCAIRO_LIBRARY AND PANGO_LIBRARY AND PANGOFT2_LIBRARY AND CAIRO_LIBRARY AND GOBJECT_LIBRARY AND GMODULE_LIBRARY AND GLIB_LIBRARY AND ART_LGPL_2_LIBRARY)
    set(RRD_LIBRARIES ${RRD_LIBRARY} ${FREETYPE_LIBRARY} ${PNG_LIBRARY} ${PANGOCAIRO_LIBRARY} ${PANGO_LIBRARY} ${PANGOFT2_LIBRARY} ${CAIRO_LIBRARY} ${GOBJECT_LIBRARY} ${GMODULE_LIBRARY} ${GLIB_LIBRARY} ${ART_LGPL_2_LIBRARY})
    set(RRD_FOUND TRUE)
    message(STATUS "Found RRDtool: ${RRD_LIBRARIES}")
  else ()
    set(RRD_FOUND FALSE)
  endif ()
endif ()

if (RRD_FOUND)
  if (NOT RRD_FIND_QUIETLY)
    message(STATUS "Found RRDtool: ${RRD_LIBRARIES}")
  endif ()
else ()
  if (RRDtool_FIND_REQUIRED)
    message(FATAL_ERROR "Could not find RRDtool library")
  endif ()
endif ()

try_run(RRD_CHECK SHOULD_COMPILE
        ${HYPERTABLE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/CMakeTmp
        ${HYPERTABLE_SOURCE_DIR}/cmake/CheckRRDtool.cc
        CMAKE_FLAGS -DINCLUDE_DIRECTORIES=${RRD_INCLUDE_DIR}
                    -DLINK_LIBRARIES=${RRD_LIBRARY}
        OUTPUT_VARIABLE RRD_TRY_OUT)
string(REGEX REPLACE ".*\n([0-9.]+).*" "\\1" RRD_VERSION ${RRD_TRY_OUT})
string(REGEX REPLACE ".*\n(RRDtool .*)" "\\1" RRD_VERSION ${RRD_VERSION})
message(STATUS "RRDtool version: ${RRD_VERSION}")

if (NOT RRD_CHECK STREQUAL "0")
  message(FATAL_ERROR "Please fix the RRDtool installation, "
          "remove CMakeCache.txt and try again.")
endif ()

STRING(REGEX REPLACE "^([0-9]+)\\.([0-9]+)\\.([0-9]+)" "\\1" MAJOR "${RRD_VERSION}")
STRING(REGEX REPLACE "^([0-9]+)\\.([0-9]+)\\.([0-9]+)" "\\2" MINOR "${RRD_VERSION}")
STRING(REGEX REPLACE "^([0-9]+)\\.([0-9]+)\\.([0-9]+)" "\\3" PATCH "${RRD_VERSION}")

set(RRD_COMPATIBLE "YES")

if (NOT RRD_COMPATIBLE)
  message(FATAL_ERROR "RRD tool version >= required." 
          "Found version ${MAJOR}.${MINOR}.${PATCH}"
          "Please fix the installation, remove CMakeCache.txt and try again.")
endif()

mark_as_advanced(
  RRD_LIBRARIES
  RRD_INCLUDE_DIR
  )
