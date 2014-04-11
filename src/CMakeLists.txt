# CMake file for hypertable source

add_subdirectory(cc/Common)
add_subdirectory(cc/HyperAppHelper)
add_subdirectory(cc/ThirdParty)
add_subdirectory(cc/AsyncComm)
add_subdirectory(cc/Hyperspace)
add_subdirectory(cc/Hypertable)
add_subdirectory(cc/Tools)
add_subdirectory(cc/FsBroker/Lib)
add_subdirectory(cc/FsBroker/local)
add_subdirectory(cc/Benchmark)

add_subdirectory(rb/Monitoring)

if (Thrift_FOUND)
  add_subdirectory(cc/ThriftBroker)
  add_subdirectory(rb/ThriftClient)
  add_subdirectory(perl/ThriftClient)
  add_subdirectory(py/ThriftClient)
  add_subdirectory(php/ThriftClient)
  add_subdirectory(java/ThriftClient)
endif ()

if (PYTHONTHRIFT_FOUND AND PYTHON_FOUND)
  add_subdirectory(cc/HyperPython)
endif ()

if (Ceph_FOUND)
   add_subdirectory(cc/FsBroker/ceph)
endif ()

if (Mapr_FOUND)
   add_subdirectory(cc/FsBroker/mapr)
endif ()

if (Qfs_FOUND)
   add_subdirectory(cc/FsBroker/qfs)
endif ()
