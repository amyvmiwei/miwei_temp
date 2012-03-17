/**
 * Copyright (C) 2007-2012 Hypertable, Inc.
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
#include <iostream>
#include <map>
#include <cassert>
#include "HyperAppHelper/Unique.h"
#include "Hypertable/Lib/Client.h"

using namespace Hypertable;
using namespace Hypertable::HyperAppHelper;

static int argc;
static char **argv;

static void 
test_generate_guid(void)
{
  std::map<String, bool> guids;

  /* this is a simple test - generate 1000 guids and make sure they're
   * all unique */
  for (int i=0; i<1000; i++) {
    String s=generate_guid();
    assert(guids.find(s)==guids.end());
    guids[s]=true;
  }
}

static void
test_create_cell_unique(void)
{
  char rowbuf[100];
  const char *schema=
        "<Schema>"
        "  <AccessGroup name=\"default\">"
        "    <ColumnFamily>"
        "      <Name>cf1</Name>"
        "      <Counter>false</Counter>"
        "      <MaxVersions>1</MaxVersions>"
        "      <TimeOrder>DESC</TimeOrder>"
        "      <deleted>false</deleted>"
        "    </ColumnFamily>"
        "    <ColumnFamily>"
        "      <Name>cf2</Name>"
        "      <Counter>false</Counter>"
        "      <deleted>false</deleted>"
        "    </ColumnFamily>"
        "  </AccessGroup>"
        "</Schema>";

  Client *client=new Client(argv[0], "./hypertable.cfg");
  NamespacePtr ns=client->open_namespace("/");

  // re-create the table
  ns->drop_table("UniqueTest", true);
  ns->create_table("UniqueTest", schema);
  TablePtr table=ns->open_table("UniqueTest");

  // insert many different GUIDs and make sure they're all unique
  std::map<String, bool> guids;
  for (int i=0; i<100; i++) {
    String guid;
    KeySpec key;
    sprintf(rowbuf, "row%d", i);
    key.row=rowbuf;
    key.row_len=strlen(rowbuf);
    key.column_family="cf1";
    create_cell_unique(table, key, guid);

    assert(guids.find(guid)==guids.end());
    guids[guid]=true;
  }

  // now do the same but this time create the cell value
  guids.clear(); 
  for (int i=100; i<200; i++) {
    String guid=generate_guid();
    String old=guid;
    KeySpec key;
    sprintf(rowbuf, "row%d", i);
    key.row=rowbuf;
    key.row_len=strlen(rowbuf);
    key.column_family="cf1";
    create_cell_unique(table, key, guid);
    assert(guids.find(guid)==guids.end());
    assert(old==guid);
    guids[guid]=true;
  }

  // negative tests: insert a cell with an unknown column
  bool caught=false;
  try {
    String guid;
    KeySpec key;
    key.row="row1";
    key.row_len=4;
    key.column_family="cf3";
    create_cell_unique(table, key, guid);
  }
  catch (Exception &ex) {
    assert(ex.code()==Error::FAILED_EXPECTATION);
    caught=true;
  }
  assert(caught==true);

  // negative tests: insert a cell in a column which is not TIME_ORDER DESC
  caught=false;
  try {
    String guid;
    KeySpec key;
    key.row="row1";
    key.row_len=4;
    key.column_family="cf2";
    create_cell_unique(table, key, guid);
  }
  catch (Exception &ex) {
    assert(ex.code()==Error::BAD_SCAN_SPEC);
    caught=true;
  }
  assert(caught==true);

  // negative tests: insert a cell twice
  caught=false;
  try {
    String guid;
    KeySpec key;
    key.row="row1"; // "row1"/"cf1" was already inserted in the tests above
    key.row_len=4;
    key.column_family="cf1";
    create_cell_unique(table, key, guid);
  }
  catch (Exception &ex) {
    assert(ex.code()==Error::ALREADY_EXISTS);
    caught=true;
  }
  assert(caught==true);
  delete client;
}

int 
main(int _argc, char **_argv)
{
  argv=_argv;
  argc=_argc;

  test_generate_guid();

  test_create_cell_unique();

  _exit(0);
}
