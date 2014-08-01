
if (JAVAC_VERSION)
  add_test(ThriftClient-java ${INSTALL_DIR}/bin/java-thrift.sh
               org.hypertable.thrift.BasicClientTest)
endif ()
