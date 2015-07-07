
#include <Common/Compat.h>
#include <Common/Error.h>
#include <Common/FileUtils.h>
#include <Common/Logger.h>
#include <Common/ClusterDefinition.h>

#include <chrono>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

using namespace Hypertable;
using namespace std;

namespace {

  const char *examples[] = {
R"(INSTALL_PREFIX=/opt/hypertable
HYPERTABLE_VERSION=0.9.8.9

role: source test00
role: master test[00-02]
role: hyperspace test[00-02]
role: spare

include: 'core.tasks')",
R"(INSTALL_PREFIX=/opt/hypertable
HYPERTABLE_VERSION=0.9.8.9

role: source test00
role: master test[00-05]
- test03
role: hyperspace test[00-02]
role: spare

include: 'core.tasks')",
R"(INSTALL_PREFIX=/opt/hypertable
HYPERTABLE_VERSION=0.9.8.9

role: source test00
role: master test[00-05] - test04

include: 'core.tasks')",
    nullptr
  };

  void display_members(ofstream &out, const string &role, vector<string> &members) {
    bool first {true};
    out << role << ": ";
    for (const auto &member : members) {
      if (!first)
        out << ", ";
      else
        first = false;
      out << member;
    }
    out << endl;
  }

}

int main(int argc, char **argv) {
  unique_ptr<ClusterDefinition::ClusterDefinition> cluster_def(new ClusterDefinition::ClusterDefinition("cluster.def"));
  vector<string> members;
  int64_t generation {};
  int64_t previous_generation {};

  {
    ofstream out("ClusterDefinition_test.output");

    for (int i=0; examples[i]; i++) {
      {
        ofstream cluster_file_out("cluster.def");
        cluster_file_out << examples[i];
      }
      cluster_def->get_role_members("master", members, &generation);
      HT_ASSERT(generation != previous_generation);
      previous_generation = generation;
      display_members(out, "master", members);
      this_thread::sleep_for(chrono::milliseconds(2000));
    }

    cluster_def->get_role_members("master", members);
    display_members(out, "master", members);

    FileUtils::unlink("cluster.def");

    cluster_def->get_role_members("master", members, &generation);
    HT_ASSERT(generation == 0);
    previous_generation = generation;
    display_members(out, "master", members);

    {
      ofstream cluster_file_out("cluster.def");
      cluster_file_out << examples[0];
    }

    cluster_def->get_role_members("master", members, &generation);
    display_members(out, "master", members);

    FileUtils::unlink("cluster.def");
  }

  string cmd = "diff ClusterDefinition_test.output ClusterDefinition_test.golden";
  if (system(cmd.c_str()) != 0)
    quick_exit(EXIT_FAILURE);

  quick_exit(EXIT_SUCCESS);
}
