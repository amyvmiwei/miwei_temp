/*
 * Copyright (C) 2007-2013 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"
#include "Common/Config.h"
#include "Common/Init.h"
#include "Common/StaticBuffer.h"
#include "Common/md5.h"

#include "AsyncComm/ConnectionManager.h"

#include "FsBroker/Lib/Client.h"

#include "Hypertable/RangeServer/AccessGroup.h"
#include "Hypertable/RangeServer/AccessGroupHintsFile.h"
#include "Hypertable/RangeServer/Global.h"

#include <cstring>

using namespace Hypertable;
using namespace std;

namespace {

  const char *bad_contents[] = {
    "",
    "foo",
    "foo: ",
    "foo: {",
    "foo: {\n  LatestStoredRevision",
    "foo: {\n  LatestStoredRevision }",
    "foo: {\n  LatestStoredRevision: }",
    ":",
    ": {",
    ": {\n  LatestStoredRevision: 0, DiskUsage: 0,  Files: 2/3/default/273AF/cs0;  }",
    "foo: {\n  LatestStoredRevision: 0,\n  DiskUsage: 0,\n  Files: 2/3/default/273AF/cs0;\n}\nbar: ",
    "foo: {\n  LatestStoredRevision: 123abc,\n  DiskUsage: 0,\n  Files: 2/3/default/273AF/cs0;\n}",
    "foo: {\n  LatestStoredRevision: 0,\n  DiskUsage: foo,\n  Files: 2/3/default/273AF/cs0;\n}",
    (const char *)0
  };

  void write_hints_file(const String &table, const String &start_row,
                        const String &end_row, const String &contents) {
    String range_dir;
    char md5DigestStr[33];
    md5_trunc_modified_base64(end_row.c_str(), md5DigestStr);
    md5DigestStr[16] = 0;
    range_dir = md5DigestStr;

    String parent_dir = format("%s/tables/%s/default/%s",
                               Global::toplevel_dir.c_str(),
                               table.c_str(), range_dir.c_str());

    if (!Global::dfs->exists(parent_dir))
      Global::dfs->mkdirs(parent_dir);

    int32_t fd = Global::dfs->create(parent_dir + "/hints",
                                     Filesystem::OPEN_FLAG_OVERWRITE, -1, -1, -1);
    StaticBuffer sbuf(contents.length());
    memcpy(sbuf.base, contents.c_str(), contents.length());
    Global::dfs->append(fd, sbuf, Filesystem::O_FLUSH);
    Global::dfs->close(fd, (DispatchHandler *)0);
  }

}

int main(int argc, char **argv) {
  try {
    InetAddr addr;
    ConnectionManagerPtr conn_mgr;
    FsBroker::ClientPtr client;

    Config::init(argc, argv);

    System::initialize(System::locate_install_dir(argv[0]));
    ReactorFactory::initialize(2);

    uint16_t port = Config::properties->get_i16("FsBroker.Port");

    InetAddr::initialize(&addr, "localhost", port);

    conn_mgr = new ConnectionManager();
    Global::dfs = new FsBroker::Client(conn_mgr, addr, 15000);

    // force broker client to be destroyed before connection manager
    client = (FsBroker::Client *)Global::dfs.get();

    if (!client->wait_for_connection(15000)) {
      HT_ERROR("Unable to connect to DFS");
      return 1;
    }

    Global::memory_tracker = new MemoryTracker(0, 0);

    Global::toplevel_dir = "/hypertable";

    // Clean up state from previous run
    if (Global::dfs->exists("/hypertable/tables/2/3/default"))
      Global::dfs->rmdir("/hypertable/tables/2/3/default");

    // Non-existant hints file read
    {
      AccessGroupHintsFile hints_file("2/3", "abc", "foo");
      hints_file.read();
      HT_ASSERT(hints_file.get().empty());
    }

    // Write/read one group
    {
      AccessGroup::Hints h;
      std::vector<AccessGroup::Hints> hints;
      AccessGroupHintsFile hints_file("2/3", "abc", "foo");

      h.ag_name = "default";
      h.latest_stored_revision = TIMESTAMP_MIN;
      h.disk_usage = 1000;
      h.files = "2/3/default/273AF/cs0;";

      hints.push_back(h);
      hints_file.set(hints);
      hints_file.write("rs1");

      hints_file.read();
      HT_ASSERT(hints_file.get().size() == 1);
      HT_ASSERT(h == hints_file.get()[0]);
    }

    // Write/read two groups
    {
      AccessGroup::Hints h1, h2;
      std::vector<AccessGroup::Hints> hints;
      AccessGroupHintsFile hints_file("2/3", "abc,", "foo");

      h1.ag_name = "foo";
      h1.latest_stored_revision = TIMESTAMP_MIN;
      h1.disk_usage = 10000;
      h1.files = "2/3/default/273AF/cs0;";
      hints.push_back(h1);

      h2.ag_name = "bar";
      h2.latest_stored_revision = 1368097650123456789LL;
      h2.disk_usage = 2000;
      h2.files = "2/3/default/A27B13F/cs0;";
      hints.push_back(h2);

      hints_file.set(hints);
      hints_file.write("rs1");

      hints.clear();
      hints_file.read();
      HT_ASSERT(hints_file.get().size() == 2);
      HT_ASSERT(h1 == hints_file.get()[0]);
      HT_ASSERT(h2 == hints_file.get()[1]);
    }

    // Write old format
    {
      AccessGroup::Hints h;
      AccessGroupHintsFile hints_file("2/3", "abc,", "foo");
      const char *fmt = "%s: {\n"
        "  LatestStoredRevision: %lld,\n"
        "  DiskUsage: %llu,\n"
        "  Files: %s\n}\n";

      h.ag_name = "bar";
      h.latest_stored_revision = 1368097650123456789LL;
      h.disk_usage = 2000;
      h.files = "2/3/default/A27B13F/cs0;";

      String contents = format(fmt, h.ag_name.c_str(), h.latest_stored_revision,
                               h.disk_usage, h.files.c_str());

      write_hints_file("2/3", "abc", "foo", contents);

      hints_file.read();

      HT_ASSERT(hints_file.get().size() == 1);
      HT_ASSERT(h == hints_file.get()[0]);
    }

    // Bad hints file contents tests
    {
      AccessGroup::Hints h;
      std::vector<AccessGroup::Hints> hints;
      AccessGroupHintsFile hints_file("2/4", "abc", "bar");
      for (size_t i=0; bad_contents[i]; i++) {
        write_hints_file("2/4", "abc", "bar", bad_contents[i]);
        hints_file.read();
        HT_ASSERT(hints_file.get().size() == 0);
      }
    }
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    _exit(1);
  }
  catch (...) {
    HT_ERROR_OUT << "unexpected exception caught" << HT_END;
    _exit(1);
  }
  _exit(0);

}

