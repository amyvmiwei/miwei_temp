/*
 * Copyright (C) 2007-2015 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License, or any later version.
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

#include <Hypertable/Lib/ColumnFamilySpec.h>

#include <Common/FileUtils.h>
#include <Common/Logger.h>
#include <Common/TestHarness.h>
#include <Common/Time.h>
#include <Common/Usage.h>

#if defined(TCMALLOC)
#include <google/tcmalloc.h>
#include <google/heap-checker.h>
#include <google/heap-profiler.h>
#include <google/malloc_extension.h>
#endif

extern "C" {
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
}

#include <cassert>
#include <cstring>

using namespace Hypertable;
using namespace std;

namespace {
  const char *usage[] = {
    "usage: ColumnFamilySpec_test [--golden]",
    "",
    "Unit test for ColumnFamilySpec class.  If --golden is supplied, a new",
    "golden output file 'ColumnFamilySpec_test.golden' will be generated",
    0
  };

  const char *bad_xml[] = {
    "<Options>\n"
    "  <MaxVersions>3\n"
    "  <TTL>1398615345</TTL>\n"
    "  <TimeOrder>desc</TimeOrder>\n"
    "  <Counter>false</Counter>\n"
    "</Options>\n",

    "<Options>\n"
    "  <MaxVersions>3</MaxVersions>\n"
    "  <TTL>1398615345</TTL>\n"
    "  <Bad>desc</Bad>\n"
    "  <Counter>false</Counter>\n"
    "</Options>\n",

    "<Options>\n"
    "  <MaxVersions>3</MaxVersions>\n"
    "  <TTL>1398615345</TTL>\n"
    "  <TimeOrder>desc</TimeOrder>\n"
    "  <Counter>false\n"
    "</Options>\n",

    "<Options>\n"
    "  <MaxVersions>-1</MaxVersions>\n"
    "</Options>\n",

    "<Options>\n"
    "  <MaxVersions>2147483649</MaxVersions>\n"
    "</Options>\n",

    "<Options>\n"
    "  <TTL>-1</TTL>\n"
    "</Options>\n",

    "<Options>\n"
    "  <TTL>2147483649</TTL>\n"
    "</Options>\n",

    "<Options>\n"
    "  <TimeOrder>bad</TimeOrder>\n"
    "</Options>\n",

    "<Options>\n"
    "  <Counter>bad</Counter>\n"
    "</Options>\n",

    nullptr
  };

}

#define TRY_BAD(_code_) do { \
  try { _code_; } \
  catch (Exception &e) { \
    string str = format("%s: %s\n", Error::get_text(e.code()), e.what()); \
    FileUtils::write(harness.get_log_file_descriptor(), str); \
    break; \
  } \
} while (0)


int main(int argc, char **argv) {
  bool golden = false;
  TestHarness harness("ColumnFamilySpec_test");

  if (argc > 1) {
    if (!strcmp(argv[1], "--golden"))
      golden = true;
    else
      Usage::dump_and_exit(usage);
  }

#if defined(TCMALLOC)
  HeapLeakChecker heap_checker("ColumnFamilySpec_test");
  {
#endif

  ColumnFamilyOptions options;
  ColumnFamilyOptions after_options;
  string str;
  HT_ASSERT(!options.is_set_max_versions());
  HT_ASSERT(!options.is_set_ttl());
  HT_ASSERT(!options.is_set_time_order_desc());
  HT_ASSERT(!options.is_set_counter());
  str = format("HQL: %s\n", options.render_hql().c_str());
  FileUtils::write(harness.get_log_file_descriptor(), str);
  str = format("<Options>\n%s</Options>\n", options.render_xml("").c_str());
  FileUtils::write(harness.get_log_file_descriptor(), str);
  TRY_BAD(after_options.parse_xml(str.c_str(), str.length()));

  options.set_max_versions(3);
  HT_ASSERT(options.is_set_max_versions());
  str = format("HQL: %s\n", options.render_hql().c_str());
  FileUtils::write(harness.get_log_file_descriptor(), str);
  str = format("<Options>\n%s</Options>\n", options.render_xml("").c_str());
  FileUtils::write(harness.get_log_file_descriptor(), str);
  after_options.parse_xml(str.c_str(), str.length());
  HT_ASSERT(options == after_options);

  options.set_time_order_desc(true);
  HT_ASSERT(options.is_set_time_order_desc());
  str = format("HQL: %s\n", options.render_hql().c_str());
  FileUtils::write(harness.get_log_file_descriptor(), str);
  str = format("<Options>\n%s</Options>\n", options.render_xml("").c_str());
  FileUtils::write(harness.get_log_file_descriptor(), str);
  after_options.parse_xml(str.c_str(), str.length());
  HT_ASSERT(options == after_options);

  options.set_ttl((time_t)1398615345);
  HT_ASSERT(options.is_set_ttl());
  str = format("HQL: %s\n", options.render_hql().c_str());
  FileUtils::write(harness.get_log_file_descriptor(), str);
  str = format("<Options>\n%s</Options>\n", options.render_xml("").c_str());
  FileUtils::write(harness.get_log_file_descriptor(), str);
  after_options.parse_xml(str.c_str(), str.length());
  HT_ASSERT(options == after_options);

  options.set_counter(false);
  HT_ASSERT(options.is_set_counter());
  str = format("HQL: %s\n", options.render_hql().c_str());
  FileUtils::write(harness.get_log_file_descriptor(), str);
  str = format("<Options>\n%s</Options>\n", options.render_xml("").c_str());
  FileUtils::write(harness.get_log_file_descriptor(), str);
  after_options.parse_xml(str.c_str(), str.length());
  HT_ASSERT(options == after_options);

  // Check bad option combinations
  options = ColumnFamilyOptions();
  TRY_BAD(options.set_max_versions(-1));
  options.set_max_versions(3);
  TRY_BAD(options.set_counter(true));
  options.set_counter(false);
  options = ColumnFamilyOptions();
  options.set_counter(true);
  TRY_BAD(options.set_max_versions(3));
  TRY_BAD(options.set_time_order_desc(true));
  options.set_time_order_desc(false);
  TRY_BAD(options.set_ttl((time_t)-1));
  options = ColumnFamilyOptions();
  options.set_time_order_desc(true);
  TRY_BAD(options.set_counter(true));
  options.set_time_order_desc(false);
  options.set_max_versions(3);
  TRY_BAD(options.set_counter(true));

  // Check bad XML
  for (size_t i=0; bad_xml[i]; ++i) {
    options = ColumnFamilyOptions();
    TRY_BAD(options.parse_xml(bad_xml[i], strlen(bad_xml[i])));
  }

  ColumnFamilyOptions defaults;
  options = ColumnFamilyOptions();
  options.set_max_versions(3);
  defaults.set_time_order_desc(true);
  options.merge(defaults);
  str = format("HQL: %s\n", options.render_hql().c_str());
  FileUtils::write(harness.get_log_file_descriptor(), str);
  str = format("<Options>\n%s</Options>\n", options.render_xml("").c_str());
  FileUtils::write(harness.get_log_file_descriptor(), str);

  defaults = ColumnFamilyOptions();
  defaults.set_counter(true);
  TRY_BAD(options.merge(defaults));

  ColumnFamilySpec *cf_spec = new ColumnFamilySpec("foo");
  HT_ASSERT(cf_spec->get_name() == "foo");
  cf_spec->set_access_group("default");
  HT_ASSERT(cf_spec->get_access_group() == "default");
  cf_spec->set_generation(123);
  HT_ASSERT(cf_spec->get_generation() == 123);
  cf_spec->set_id(42);
  HT_ASSERT(cf_spec->get_id() == 42);
  cf_spec->set_value_index(true);
  HT_ASSERT(cf_spec->get_value_index());
  HT_ASSERT(cf_spec->get_generation() == 0);
  cf_spec->set_generation(123);
  cf_spec->set_qualifier_index(true);
  HT_ASSERT(cf_spec->get_qualifier_index());
  HT_ASSERT(cf_spec->get_generation() == 0);
  cf_spec->set_generation(123);
  cf_spec->set_deleted(true);
  HT_ASSERT(cf_spec->get_deleted());
  HT_ASSERT(cf_spec->get_generation() == 0);

  str = cf_spec->render_xml("", true);
  FileUtils::write(harness.get_log_file_descriptor(), str);
  str = cf_spec->render_hql();
  if (!str.empty())
    str += "\n";
  FileUtils::write(harness.get_log_file_descriptor(), str);
  cf_spec->set_deleted(false);
  str = cf_spec->render_hql();
  if (!str.empty())
    str += "\n";
  FileUtils::write(harness.get_log_file_descriptor(), str);

  cf_spec->set_option_max_versions(3);
  cf_spec->set_option_ttl((time_t)1398615345);
  cf_spec->set_option_time_order_desc(true);
  cf_spec->set_deleted(false);
  cf_spec->set_value_index(false);
  cf_spec->set_generation(123);

  str = cf_spec->render_xml("", true);
  FileUtils::write(harness.get_log_file_descriptor(), str);
  ColumnFamilySpec new_spec;
  new_spec.parse_xml(str.c_str(), str.length());
  HT_ASSERT(new_spec == *cf_spec);
  string str_after = new_spec.render_xml("", true);
  HT_ASSERT(str == str_after);

  str = cf_spec->render_hql();
  if (!str.empty())
    str += "\n";
  FileUtils::write(harness.get_log_file_descriptor(), str);

  delete cf_spec;
  cf_spec = new ColumnFamilySpec("bar");
  cf_spec->set_option_counter(true);
  TRY_BAD(cf_spec->set_value_index(true));
  TRY_BAD(cf_spec->set_qualifier_index(true));
  cf_spec->set_option_counter(false);
  cf_spec->set_value_index(true);
  TRY_BAD(cf_spec->set_option_counter(true));
  cf_spec->set_value_index(false);
  cf_spec->set_qualifier_index(true);
  TRY_BAD(cf_spec->set_option_counter(true));
  delete cf_spec;

  if (!golden)
    harness.validate_and_exit("ColumnFamilySpec_test.golden");

  harness.regenerate_golden_file("ColumnFamilySpec_test.golden");

#if defined(TCMALLOC)
  }
  if (!heap_checker.NoLeaks()) assert(!"heap memory leak");
#endif

  return 0;
}
