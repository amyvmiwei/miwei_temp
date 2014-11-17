
#include <Common/Compat.h>
#include <Common/Error.h>
#include <Common/HostSpecification.h>
#include <Common/Logger.h>

#include <fstream>
#include <iostream>

using namespace Hypertable;
using namespace std;

namespace {

  const char *valid[] = {
    "test1 test2 test[1-2]a test[1-2]b",
    "host[01-36] - host[20-35]",
    "(host[01-36].hypertable.com - host17.hypertable.com) - host[20-35].hypertable.com",
    "(host[01-15].hypertable.com) + (host[20-35].hypertable.com - host29.hypertable.com)",
    "host[1-17]",
    "host[7-9] + host10",
    "host[7-9]\nhost10",
    "host1, host2, host3",
    "host[01-10] - host07",
    "host[01-10] -host07",
    "ht-rel-css-slave[21-25]",
    nullptr
  };

  const char *invalid[] = {
    "test1 test~2 test3",
    "test[a-07]",
    "test[0a-07]",
    "test[08]",
    "test[08-a]",
    "test[08-020]",
    "test[",
    "test[08",
    "test[08-",
    "test[08-10",
    "test[0	8-10]",
    "- host[01-30]",
    "+ host[01-30]",
    "(- host[01-30])",
    "(+ host[01-30])",
    "host[01-30] -",
    "host[01-30] +",
    "(host[01-30] -)",
    "(host[01-30] +)",
    nullptr
  };

}

int main(int argc, char **argv) {

  {
    ofstream out("HostSpecification_test.output");

    vector<string> hosts;
    for (size_t i=0; valid[i]; i++) {
      out << "# " << valid[i] << "\n";
      hosts = HostSpecification(valid[i]);
      for (auto & host : hosts)
        out << host << " ";
      out << endl;
    }

    for (size_t i=0; invalid[i]; i++) {
      out << "# " << invalid[i] << "\n";
      try {
        hosts = HostSpecification(invalid[i]);
      }
      catch (Exception &e) {
        out << "Invalid host specification: " << e.what() << "\n";
      }
    }

    char buf[8];
    strcpy(buf, "test[?");
    char *ptr = &buf[5];
    for (size_t i=1; i<32; i++) {
      *ptr = (char)i;
      try {
        hosts = HostSpecification(buf);
      }
      catch (Exception &e) {
        out << "Invalid host specification: " << e.what() << "\n";
      }
    }
    out << flush;
  }

  string cmd = "diff HostSpecification_test.output HostSpecification_test.golden";
  if (system(cmd.c_str()) != 0)
    _exit(1);

  return 0;

}
