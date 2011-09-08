# - Find Snappy 
# Find the snappy compression library and includes
#
#  SNAPPY_INCLUDE_DIR - where to find snappy.h, etc.
#  SNAPPY_LIBRARIES   - List of libraries when using snappy.
#  SNAPPY_FOUND       - True if snappy found.

find_path(SNAPPY_INCLUDE_DIR snappy.h NO_DEFAULT_PATH PATHS
  ${HT_DEPENDENCY_INCLUDE_DIR}
  /usr/include
  /opt/local/include
  /usr/local/include
)

set(SNAPPY_NAMES ${SNAPPY_NAMES} snappy)
find_library(SNAPPY_LIBRARY NAMES ${SNAPPY_NAMES} NO_DEFAULT_PATH PATHS
    ${HT_DEPENDENCY_LIB_DIR}
    /usr/local/lib
    /opt/local/lib
    /usr/lib
    )

if (SNAPPY_INCLUDE_DIR AND SNAPPY_LIBRARY)
  set(SNAPPY_FOUND TRUE)
  set( SNAPPY_LIBRARIES ${SNAPPY_LIBRARY} )
else ()
  set(SNAPPY_FOUND FALSE)
  set( SNAPPY_LIBRARIES )
endif ()

if (SNAPPY_FOUND)
  message(STATUS "Found Snappy: ${SNAPPY_LIBRARY}")
  try_run(SNAPPY_CHECK SNAPPY_CHECK_BUILD
          ${HYPERTABLE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/CMakeTmp
          ${HYPERTABLE_SOURCE_DIR}/cmake/CheckSnappy.cc
          CMAKE_FLAGS -DINCLUDE_DIRECTORIES=${SNAPPY_INCLUDE_DIR}
                      -DLINK_LIBRARIES=${SNAPPY_LIBRARIES}
          OUTPUT_VARIABLE SNAPPY_TRY_OUT)
  if (SNAPPY_CHECK_BUILD AND NOT SNAPPY_CHECK STREQUAL "0")
    string(REGEX REPLACE ".*\n(SNAPPY .*)" "\\1" SNAPPY_TRY_OUT ${SNAPPY_TRY_OUT})
    message(STATUS "${SNAPPY_TRY_OUT}")
    message(FATAL_ERROR "Please fix the Snappy installation and try again.")
    set(SNAPPY_LIBRARIES)
  endif ()
  string(REGEX REPLACE ".*\n([0-9]+[^\n]+).*" "\\1" SNAPPY_VERSION ${SNAPPY_TRY_OUT})
  if (NOT SNAPPY_VERSION MATCHES "^[0-9]+.*")
    set(SNAPPY_VERSION "unknown") 
  endif ()
  message(STATUS "       version: ${SNAPPY_VERSION}")
else ()
  message(STATUS "Not Found Snappy: ${SNAPPY_LIBRARY}")
  if (SNAPPY_FIND_REQUIRED)
    message(STATUS "Looked for Snappy libraries named ${SNAPPY_NAMES}.")
    message(FATAL_ERROR "Could NOT find Snappy library")
  endif ()
endif ()

mark_as_advanced(
  SNAPPY_LIBRARY
  SNAPPY_INCLUDE_DIR
  )
