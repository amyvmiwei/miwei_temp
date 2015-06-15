/*
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

#include <Common/Compat.h>

#include <Hypertable/Lib/Client.h>

#include <iostream>
#include <map>
#include <cassert>

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
        "      <Index>true</Index>"
        "    </ColumnFamily>"
        "    <ColumnFamily>"
        "      <Name>v</Name>"
        "      <Index>true</Index>"
        "      <QualifierIndex>true</QualifierIndex>"
        "    </ColumnFamily>"
        "    <ColumnFamily>"
        "      <Name>r</Name>"
        "    </ColumnFamily>"
        "    <ColumnFamily>"
        "      <Name>rb</Name>"
        "    </ColumnFamily>"
        "  </AccessGroup>"
        "</Schema>";

static size_t
value_len(const char* value) {
  return strlen(value) + 1;
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
  for (auto &q : queries) {
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
    exit(EXIT_FAILURE);
  }
}

void
assert_scan(
  int expected_cell_count,
  TablePtr table,
  const char* column,
  const RowInterval ri[],
  int ri_count,
  const ColumnPredicate& cp,
  std::function<bool(const Cell&)> validate)
{
  ScanSpecBuilder ssb;
  ssb.add_column(column);
  if (ri && ri_count) {
    for (int i = 0; i < ri_count; ++i)
      ssb.add_row_interval(
        ri[i].start, ri[i].start_inclusive,
        ri[i].end, ri[i].end_inclusive);
  }
  ssb.add_column_predicate(
    cp.column_family,
    cp.column_qualifier,
    cp.operation,
    cp.value,
    cp.value_len);

  int cell_count = 0;
  {
    TableScannerPtr s(table->create_scanner(ssb.get()));
    Cell cell;
    while (s->next(cell)) {
      HT_ASSERT(validate(cell));
      ++cell_count;
    }
  }

  HT_ASSERT(expected_cell_count == cell_count);
}

static void
test_column_predicate(void)
{
  const int search_int = 99;
  const char* search_cstr = "99";

  TablePtr table = ht_namespace->open_table("IndexTest");
  {
    char rowbuf[100];
    char valbuf[100];

    TableMutatorPtr tm(table->create_mutator());

    for (int i = 0; i < 10000; i++) {
      int mod100 = i % 100;

      KeySpec key;

      sprintf(rowbuf, "%06d", i);
      key.row=rowbuf;
      key.row_len=strlen(rowbuf);

      // add binary values
      key.column_family = "rb";
      tm->set(key, &i, sizeof(i));

      key.column_family = "a";
      tm->set(key, &i, sizeof(i));

      key.column_qualifier = "mod100_bin";
      key.column_qualifier_len = strlen(key.column_qualifier);
      tm->set(key, &mod100, sizeof(mod100));

      // add qualifier
      if ((i % 1000) == 0) {
        key.column_family = "v";
        key.column_qualifier = "zero1000";
        key.column_qualifier_len = strlen(key.column_qualifier);
        tm->set(key, 0, 0);
      }

      if ((i % 3333) == 0) {
        key.column_family = "v";
        key.column_qualifier = "zero3333";
        key.column_qualifier_len = strlen(key.column_qualifier);
        tm->set(key, 0, 0);
      }

      key.column_qualifier = 0;
      key.column_qualifier_len = 0;

      // add string values
      key.column_family = "r";
      sprintf(valbuf, "%d", i);
      tm->set(key, valbuf, value_len(valbuf));

      key.column_family = "v";
      tm->set(key, valbuf, value_len(valbuf));

      sprintf(valbuf, "%d", mod100);
      key.column_qualifier = "mod100";
      key.column_qualifier_len = strlen(key.column_qualifier);
      tm->set(key, valbuf, value_len(valbuf));
    }

    {
      KeySpec key;

      // add extra rows
      strcpy(rowbuf, "test-1");
      key.row = rowbuf;
      key.row_len = strlen(rowbuf);
      key.column_family = "rb";
      tm->set(key, &search_int, sizeof(search_int));
      key.column_family = "a";
      tm->set(key, &search_int, sizeof(search_int));

      key.column_family = "r";
      tm->set(key, search_cstr, value_len(search_cstr));
      key.column_family = "v";
      tm->set(key, search_cstr, value_len(search_cstr));

      strcpy(rowbuf, "test-2");
      key.row = rowbuf;
      key.row_len = strlen(rowbuf);
      key.column_family = "rb";
      tm->set(key, &search_int, sizeof(search_int));
      key.column_family = "a";
      key.column_qualifier = "mod1000";
      key.column_qualifier_len = strlen(key.column_qualifier);
      tm->set(key, &search_int, sizeof(search_int));

      key.column_family = "r";
      tm->set(key, search_cstr, value_len(search_cstr));
      key.column_family = "v";
      key.column_qualifier = "mod1000";
      key.column_qualifier_len = strlen(key.column_qualifier);
      tm->set(key, search_cstr, value_len(search_cstr));

      strcpy(rowbuf, "test-3");
      key.row = rowbuf;
      key.row_len = strlen(rowbuf);
      key.column_family = "v";
      key.column_qualifier = "tab1\ttab1";
      key.column_qualifier_len = strlen(key.column_qualifier);
      tm->set(key, 0, 0);
      key.column_family = "r";
      tm->set(key, key.column_qualifier, value_len(key.column_qualifier));

      strcpy(rowbuf, "test-4");
      key.row = rowbuf;
      key.row_len = strlen(rowbuf);
      key.column_family = "v";
      key.column_qualifier = "tab2\ttab2";
      key.column_qualifier_len = strlen(key.column_qualifier);
      tm->set(key, 0, 0);
      key.column_family = "r";
      tm->set(key, key.column_qualifier, value_len(key.column_qualifier));
    }
  }

  // exact value match, accross column qualifiers
  assert_scan(
    102,
    table,
    "rb",
    0, 0,
    ColumnPredicate(
      "a", 0, ColumnPredicate::EXACT_MATCH,
      reinterpret_cast<const char*>(&search_int), sizeof(search_int)),
    [](const Cell& cell) {
      return cell.value_len == sizeof(int) &&
              (*reinterpret_cast<const int*>(cell.value) % 100) == 99;
  });

  assert_scan(
    102,
    table,
    "r",
    0, 0,
    ColumnPredicate(
      "v", 0, ColumnPredicate::EXACT_MATCH,
      search_cstr, value_len(search_cstr)),
    [](const Cell& cell) {
      return (atoi(reinterpret_cast<const char*>(cell.value)) % 100) == 99;
  });

  // exact value match and exact qualifier match
  assert_scan(
    100,
    table,
    "rb",
    0, 0,
    ColumnPredicate(
      "a", "mod100_bin",
      ColumnPredicate::EXACT_MATCH|ColumnPredicate::QUALIFIER_EXACT_MATCH,
      reinterpret_cast<const char*>(&search_int), sizeof(search_int)),
    [](const Cell& cell) {
      return (atoi(cell.row_key) % 100) == 99 &&
              cell.value_len == sizeof(int) &&
              (*reinterpret_cast<const int*>(cell.value) % 100) == 99;
  });

  assert_scan(
    100,
    table,
    "r",
    0, 0,
    ColumnPredicate(
      "v", "mod100",
      ColumnPredicate::EXACT_MATCH|ColumnPredicate::QUALIFIER_EXACT_MATCH,
      search_cstr, value_len(search_cstr)),
    [](const Cell& cell) {
      return (atoi(cell.row_key) % 100) == 99 &&
              (atoi(reinterpret_cast<const char*>(cell.value)) % 100) == 99;
  });

  assert_scan(
    2,
    table,
    "rb",
    0, 0,
    ColumnPredicate(
      "a", "",
      ColumnPredicate::EXACT_MATCH|ColumnPredicate::QUALIFIER_EXACT_MATCH,
      reinterpret_cast<const char*>(&search_int), sizeof(search_int)),
    [](const Cell& cell) {
      return cell.value_len == sizeof(int) &&
              *reinterpret_cast<const int*>(cell.value) == 99;
  });

  assert_scan(
    2,
    table,
    "r",
    0, 0,
    ColumnPredicate(
      "v", "",
      ColumnPredicate::EXACT_MATCH|ColumnPredicate::QUALIFIER_EXACT_MATCH,
      search_cstr, value_len(search_cstr)),
    [](const Cell& cell) {
      return atoi(reinterpret_cast<const char*>(cell.value)) == 99;
  });

  // exact value match and qualifier preffix match
  assert_scan(
    101,
    table,
    "rb",
    0, 0,
    ColumnPredicate(
      "a", "mod1", ColumnPredicate::EXACT_MATCH|ColumnPredicate::QUALIFIER_PREFIX_MATCH,
      reinterpret_cast<const char*>(&search_int), sizeof(search_int)),
    [](const Cell& cell) {
      return cell.value_len == sizeof(int) &&
              (*reinterpret_cast<const int*>(cell.value) % 100) == 99;
  });

  assert_scan(
    101,
    table,
    "r",
    0, 0,
    ColumnPredicate(
      "v", "mod1", ColumnPredicate::EXACT_MATCH|ColumnPredicate::QUALIFIER_PREFIX_MATCH,
      search_cstr, value_len(search_cstr)),
    [](const Cell& cell) {
      return (atoi(reinterpret_cast<const char*>(cell.value)) % 100) == 99;
  });

  // exact value match and qualifier regex match
  assert_scan(
    101,
    table,
    "rb",
    0, 0,
    ColumnPredicate(
      "a", "^mo.*", ColumnPredicate::EXACT_MATCH|ColumnPredicate::QUALIFIER_REGEX_MATCH,
      reinterpret_cast<const char*>(&search_int), sizeof(search_int)),
    [](const Cell& cell) {
      return cell.value_len == sizeof(int) &&
              (*reinterpret_cast<const int*>(cell.value) % 100) == 99;
  });

  assert_scan(
    101,
    table,
    "r",
    0, 0,
    ColumnPredicate(
      "v", "^mo.*", ColumnPredicate::EXACT_MATCH|ColumnPredicate::QUALIFIER_REGEX_MATCH,
      search_cstr, value_len(search_cstr)),
    [](const Cell& cell) {
      return (atoi(reinterpret_cast<const char*>(cell.value)) % 100) == 99;
  });

  // exact qualifier match
  assert_scan(
    10,
    table,
    "rb",
    0, 0,
    ColumnPredicate(
      "v", "zero1000", ColumnPredicate::QUALIFIER_EXACT_MATCH, 0, 0),
    [](const Cell& cell) {
      return cell.value_len == sizeof(int) &&
              (*reinterpret_cast<const int*>(cell.value) % 1000) == 0;
  });

  // preffix qualifier match
  assert_scan(
    13,
    table,
    "rb",
    0, 0,
    ColumnPredicate(
      "v", "zero", ColumnPredicate::QUALIFIER_PREFIX_MATCH, 0, 0),
    [](const Cell& cell) {
      return cell.value_len == sizeof(int) &&
            ((*reinterpret_cast<const int*>(cell.value) % 1000) == 0 ||
              (*reinterpret_cast<const int*>(cell.value) % 3333) == 0);
  });

  // preffix regex match
  assert_scan(
    10,
    table,
    "rb",
    0, 0,
    ColumnPredicate(
      "v", "^ze..1.*", ColumnPredicate::QUALIFIER_REGEX_MATCH, 0, 0),
    [](const Cell& cell) {
      return cell.value_len == sizeof(int) &&
            (*reinterpret_cast<const int*>(cell.value) % 1000) == 0;
  });

  // preffix regex match, escaped
  assert_scan(
    2,
    table,
    "r",
    0, 0,
    ColumnPredicate(
      "v", "^tab.*", ColumnPredicate::QUALIFIER_REGEX_MATCH, 0, 0),
    [](const Cell& cell) {
      const char* s = reinterpret_cast<const char*>(cell.value);
      return strstr(s, "tab") == s;
  });

  assert_scan(
    1,
    table,
    "r",
    0, 0,
    ColumnPredicate(
      "v", "^tab1\t.*", ColumnPredicate::QUALIFIER_REGEX_MATCH, 0, 0),
    [](const Cell& cell) {
      const char* s = reinterpret_cast<const char*>(cell.value);
      return strstr(s, "tab1\t") == s;
  });

  assert_scan(
    2,
    table,
    "r",
    0, 0,
    ColumnPredicate(
      "v", "^tab[1|2]\t.*", ColumnPredicate::QUALIFIER_REGEX_MATCH, 0, 0),
    [](const Cell& cell) {
      const char* s = reinterpret_cast<const char*>(cell.value);
      return strstr(s, "tab") == s;
  });

  // exact value match, exact qualifier match and row intervals
  {
    const RowInterval ri("000297", true, "000500", true);

    assert_scan(
      3,
      table,
      "rb",
      &ri, 1,
      ColumnPredicate(
        "a", "mod100_bin",
        ColumnPredicate::EXACT_MATCH|ColumnPredicate::QUALIFIER_EXACT_MATCH,
        reinterpret_cast<const char*>(&search_int), sizeof(search_int)),
      [](const Cell& cell) {
        return (atoi(cell.row_key) % 100) == 99 &&
                cell.value_len == sizeof(int) &&
                (*reinterpret_cast<const int*>(cell.value) % 100) == 99;
    });

    assert_scan(
      3,
      table,
      "r",
      &ri, 1,
      ColumnPredicate(
        "v", "mod100",
        ColumnPredicate::EXACT_MATCH|ColumnPredicate::QUALIFIER_EXACT_MATCH,
        search_cstr, value_len(search_cstr)),
      [](const Cell& cell) {
        return (atoi(cell.row_key) % 100) == 99 &&
                (atoi(reinterpret_cast<const char*>(cell.value)) % 100) == 99;
    });
  }

  {
    const RowInterval ri[] = {
      RowInterval("000297", true, "000500", true),
      RowInterval("009297", true, "009500", true)
    };

    assert_scan(
      6,
      table,
      "rb",
      ri, 2,
      ColumnPredicate(
        "a", "mod100_bin",
        ColumnPredicate::EXACT_MATCH|ColumnPredicate::QUALIFIER_EXACT_MATCH,
        reinterpret_cast<const char*>(&search_int), sizeof(search_int)),
      [](const Cell& cell) {
        return (atoi(cell.row_key) % 100) == 99 &&
                cell.value_len == sizeof(int) &&
                (*reinterpret_cast<const int*>(cell.value) % 100) == 99;
    });

    assert_scan(
      6,
      table,
      "r",
      ri, 2,
      ColumnPredicate(
        "v", "mod100",
        ColumnPredicate::EXACT_MATCH|ColumnPredicate::QUALIFIER_EXACT_MATCH,
        search_cstr, value_len(search_cstr)),
      [](const Cell& cell) {
        return (atoi(cell.row_key) % 100) == 99 &&
                (atoi(reinterpret_cast<const char*>(cell.value)) % 100) == 99;
    });
  }

  // exact qualifier match and row intervals
  {
    const RowInterval ri("002997", true, "005000", true);

    assert_scan(
      3,
      table,
      "rb",
      &ri, 1,
      ColumnPredicate(
        "v", "zero1000", ColumnPredicate::QUALIFIER_EXACT_MATCH, 0, 0),
      [](const Cell& cell) {
        return cell.value_len == sizeof(int) &&
                (*reinterpret_cast<const int*>(cell.value) % 1000) == 0;
    });
  }

  // exact qualifier match and row intervals
  {
    const RowInterval ri[] = {
      RowInterval("002997", true, "005000", true),
      RowInterval("009297", true, "009500", true)
    };

    assert_scan(
      3,
      table,
      "rb",
      ri, 2,
      ColumnPredicate(
        "v", "zero1000", ColumnPredicate::QUALIFIER_EXACT_MATCH, 0, 0),
      [](const Cell& cell) {
        return cell.value_len == sizeof(int) &&
                (*reinterpret_cast<const int*>(cell.value) % 1000) == 0;
    });
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

  ht_namespace->drop_table("IndexTest", true);
  ht_namespace->create_table("IndexTest", schema);
  test_column_predicate();

  ht_namespace = 0; // delete namespace before ht_client goes out of scope
  delete ht_client;
  return (0);
}
