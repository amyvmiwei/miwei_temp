/**
 * Copyright (C) 2011 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
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
#include "Hypertable/Lib/Client.h"

using namespace Hypertable;

static int argc;
static char **argv;
static Client *ht_client;
static NamespacePtr ht_namespace;
static const char *schema=
        "<Schema>"
        "  <AccessGroup name=\"default\">"
        "    <ColumnFamily>"
        "      <Name>a</Name>"
        "      <Counter>false</Counter>"
        "      <deleted>false</deleted>"
        "      <Index>true</Index>"
        "    </ColumnFamily>"
        "  </AccessGroup>"
        "</Schema>";


static void
test_insert_timestamps(void)
{
  char rowbuf[100];
  char valbuf[100];
  TablePtr table=ht_namespace->open_table("IndexTest");
  TableMutator *tm=table->create_mutator();

  // insert a couple of keys WITH timestamp
  for (int i=0; i<100; i++) {
    sprintf(valbuf, "val%03d", i);
    KeySpec key;
    sprintf(rowbuf, "row%03d", i);
    key.row=rowbuf;
    key.row_len=strlen(rowbuf);
    key.column_family="a";
    key.timestamp=i;
    tm->set(key, valbuf);
  }

  delete tm;

  // now verify that the index keys have the same timestamp
  ScanSpecBuilder ssb;
  ssb.add_column("v1");
  TableScanner *ts = table->get_index_table()->create_scanner(ssb.get());
  Cell cell;
  int i = 0;
  while (ts->next(cell)) {
    char buf[100];
    sprintf(buf, "1,val%03d\t\trow%03d", i, i);
    HT_ASSERT(strcmp(buf, cell.row_key)==0);
    HT_ASSERT(cell.value_len==0);
    HT_ASSERT(cell.timestamp==i);
    i++;
  }

  delete ts;

  // re-create the table
  ht_namespace->drop_table("IndexTest", true);
  ht_namespace->create_table("IndexTest", schema);
  table=ht_namespace->open_table("IndexTest");
  tm=table->create_mutator();

  // insert a couple of keys WITHOUT timestamp
  for (int i = 0; i < 100; i++) {
    sprintf(valbuf, "val%03d", i);
    KeySpec key;
    sprintf(rowbuf, "row%03d", i);
    key.row=rowbuf;
    key.row_len=strlen(rowbuf);
    key.column_family="a";
    key.timestamp=i;
    tm->set(key, valbuf);
  }

  delete tm;

  // now verify that the index keys have the same timestamp
  ScanSpecBuilder ssbidx;
  ssbidx.add_column("v1");
  TableScanner *tsi=table->get_index_table()->create_scanner(ssbidx.get());
  ScanSpecBuilder ssbprim;
  ssbprim.add_column("a");
  TableScanner *tsp=table->create_scanner(ssbprim.get());

  Cell idx_cell;
  i = 0;
  while (tsi->next(idx_cell) && tsp->next(cell)) {
    char buf[100];
    sprintf(buf, "1,val%03d\t\trow%03d", i, i);
    HT_ASSERT(strcmp(buf, idx_cell.row_key)==0);
    HT_ASSERT(idx_cell.value_len==0);
    HT_ASSERT(idx_cell.timestamp==cell.timestamp);
    i++;
  }

  delete tsi;
  delete tsp;
}

static String
escape(const char *str, size_t len) 
{
  String ret;
  for (size_t i = 0; i < len; i++) {
    if (str[i] == '\t')
      ret += "\\t";
    else if (str[i] == '\0')
      ret += "\\0";
    else
      ret += str[i];
  }

  return ret;
}

static void
test_escaped_regexps(void)
{
  char rowbuf[100];
  TablePtr table = ht_namespace->open_table("IndexTest");
  TableMutator *tm = table->create_mutator();
  const char *values[] = {
      "\tcell",
      "cell\ta1",
      "cell\ta2\t",
      "cell\ta12\t",
      "\t", 
      "\t\t", 
      0
  };

  // insert a couple of keys WITH timestamp
  int i = 0;
  for (const char **v = &values[0]; *v; v++, i++) {
    KeySpec key;
    sprintf(rowbuf, "row%03d", i);
    key.row=rowbuf;
    key.row_len=strlen(rowbuf);
    key.column_family="a";
    tm->set(key, *v);
  }

  // one more value with a zero-byte
  KeySpec key;
  sprintf(rowbuf, "row%03d", i);
  key.row=rowbuf;
  key.row_len=strlen(rowbuf);
  key.column_family="a";
  const char *v="ce\0ll00"; 
  tm->set(key, v, 7);

  delete tm;

  typedef std::pair<const char *, int> Query;
  std::vector<Query> queries;
  queries.push_back(Query("\tcell", ColumnPredicate::EXACT_MATCH));
  queries.push_back(Query("cell\ta1", ColumnPredicate::EXACT_MATCH));
  queries.push_back(Query("cell\ta2\t", ColumnPredicate::EXACT_MATCH));
  queries.push_back(Query("cell\ta12\t", ColumnPredicate::EXACT_MATCH));
  queries.push_back(Query("\t", ColumnPredicate::EXACT_MATCH));
  queries.push_back(Query("\t\t", ColumnPredicate::EXACT_MATCH));
  queries.push_back(Query("ce", ColumnPredicate::PREFIX_MATCH));

  // now make sure that the values were correctly escaped
  Cell cell;
  FILE *fout=fopen("indices_test.output", "wt");
  foreach_ht (Query &q, queries) {
    ScanSpecBuilder ssb;
    ssb.add_column_predicate("a", "", q.second, q.first);
    String s=escape(q.first, strlen(q.first));
    //String s(q.first);
    fprintf(fout, "query: '%s'\n", s.c_str());
    TableScanner *ts=table->create_scanner(ssb.get());
    int j = 0;
    while (ts->next(cell)) {
      //String s((const char *)cell.value, (size_t)cell.value_len);
      s=escape((const char *)cell.value, (size_t)cell.value_len);
      fprintf(fout, "  %d: '%s'\n", j++, s.c_str());
    }
    delete ts;
  }
  fclose(fout);

  const char *cmd = "diff indices_test.output indices_test.golden";
  if (system(cmd) != 0) {
    printf("diff failed\n");
    exit(1);
  }
}

int 
main(int _argc, char **_argv)
{
  argv=_argv;
  argc=_argc;

  ht_client=new Client(argv[0], "./hypertable.cfg");
  ht_namespace=ht_client->open_namespace("/");

  ht_namespace->drop_table("IndexTest", true);
  ht_namespace->create_table("IndexTest", schema);
  test_insert_timestamps();

  ht_namespace->drop_table("IndexTest", true);
  ht_namespace->create_table("IndexTest", schema);
  test_escaped_regexps();

  ht_namespace = 0; // delete namespace before ht_client goes out of scope
  delete ht_client;
  return (0);
}
