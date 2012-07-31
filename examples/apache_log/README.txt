
To compile this example, you will need to modify the following
variables in the Makefile:

BOOST_INCLUDE_DIR=/usr/local/include/boost-1_34_1
BOOST_THREAD_LIB=boost_thread-gcc41-mt
HYPERTABLE_INSTALL_DIR=/opt/hypertable/current

They should be modified to reflect your particular Boost
installation and Hypertable installation.

You can then build the example by just typing 'make'

The servers can be started with the following command
(assuming Hypertable is installed in /opt/hypertable/current):

$ /opt/hypertable/current/bin/start-all-servers.sh local

The next step is to create the 'LogDb' table
with the following command:

$ /opt/hypertable/current/bin/ht shell --namespace /

...

CREATE TABLE LogDb (
  ClientIpAddress,
  UserId,
  Request,
  ResponseCode,
  ObjectSize,
  Referer,
  UserAgent
);

There is a compressed sample Apache web log in the file 'access.log.gz'.

You can load it into the table with the following command:

$ ./apache_log_load access.log.gz

You can then query it with a something like the following:

$ ./apache_log_query /favicon.ico
