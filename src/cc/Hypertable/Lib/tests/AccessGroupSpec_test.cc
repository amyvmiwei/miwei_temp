/*
 * Copyright (C) 2007-2014 Hypertable, Inc.
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

#include <Hypertable/Lib/AccessGroupSpec.h>

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

using namespace Hypertable;
using namespace std;

namespace {
  const char *usage[] = {
    "usage: AccessGroupSpec_test [--golden]",
    "",
    "Unit test for AccessGroupSpec class.  If --golden is supplied, a new",
    "golden output file 'AccessGroupSpec_test.golden' will be generated",
    0
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
  TestHarness harness("AccessGroupSpec_test");

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

  AccessGroupOptions options;
  AccessGroupOptions after_options;
  string str;
  HT_ASSERT(!options.is_set_replication());
  HT_ASSERT(!options.is_set_blocksize());
  HT_ASSERT(!options.is_set_compressor());
  HT_ASSERT(!options.is_set_bloom_filter());
  HT_ASSERT(!options.is_set_in_memory());
  str = format("HQL: %s\n", options.render_hql().c_str());
  FileUtils::write(harness.get_log_file_descriptor(), str);
  str = format("<Options>\n%s</Options>\n", options.render_xml("").c_str());
  FileUtils::write(harness.get_log_file_descriptor(), str);

  options.set_replication(3);
  HT_ASSERT(options.is_set_replication());
  str = format("HQL: %s\n", options.render_hql().c_str());
  FileUtils::write(harness.get_log_file_descriptor(), str);
  str = format("<Options>\n%s</Options>\n", options.render_xml("").c_str());
  FileUtils::write(harness.get_log_file_descriptor(), str);
  after_options.parse_xml(str.c_str(), str.length());
  HT_ASSERT(options == after_options);

  options.set_blocksize(67108864);
  HT_ASSERT(options.is_set_blocksize());
  str = format("HQL: %s\n", options.render_hql().c_str());
  FileUtils::write(harness.get_log_file_descriptor(), str);
  str = format("<Options>\n%s</Options>\n", options.render_xml("").c_str());
  FileUtils::write(harness.get_log_file_descriptor(), str);
  after_options.parse_xml(str.c_str(), str.length());
  HT_ASSERT(options == after_options);

  options.set_compressor("zlib --best");
  HT_ASSERT(options.is_set_compressor());
  str = format("HQL: %s\n", options.render_hql().c_str());
  FileUtils::write(harness.get_log_file_descriptor(), str);
  str = format("<Options>\n%s</Options>\n", options.render_xml("").c_str());
  FileUtils::write(harness.get_log_file_descriptor(), str);
  after_options.parse_xml(str.c_str(), str.length());
  HT_ASSERT(options == after_options);

  options.set_bloom_filter("rows+cols --false-positive 0.02 --bits-per-item 9 --num-hashes 7 --max-approx-items 900");
  HT_ASSERT(options.is_set_bloom_filter());
  str = format("HQL: %s\n", options.render_hql().c_str());
  FileUtils::write(harness.get_log_file_descriptor(), str);
  str = format("<Options>\n%s</Options>\n", options.render_xml("").c_str());
  FileUtils::write(harness.get_log_file_descriptor(), str);
  after_options.parse_xml(str.c_str(), str.length());
  HT_ASSERT(options == after_options);

  options.set_in_memory(true);
  HT_ASSERT(options.is_set_in_memory());
  str = format("HQL: %s\n", options.render_hql().c_str());
  FileUtils::write(harness.get_log_file_descriptor(), str);
  str = format("<Options>\n%s</Options>\n", options.render_xml("").c_str());
  FileUtils::write(harness.get_log_file_descriptor(), str);
  after_options.parse_xml(str.c_str(), str.length());
  HT_ASSERT(options == after_options);

  AccessGroupOptions defaults;
  options = AccessGroupOptions();
  after_options = AccessGroupOptions();
  options.set_replication(3);
  defaults.set_in_memory(true);
  options.merge(defaults);
  str = format("HQL: %s\n", options.render_hql().c_str());
  FileUtils::write(harness.get_log_file_descriptor(), str);
  str = format("<Options>\n%s</Options>\n", options.render_xml("").c_str());
  FileUtils::write(harness.get_log_file_descriptor(), str);
  after_options.parse_xml(str.c_str(), str.length());
  HT_ASSERT(options == after_options);

  AccessGroupSpec *ag_spec = new AccessGroupSpec("default");
  HT_ASSERT(ag_spec->get_name() == "default");
  ag_spec->set_option_replication(3);
  HT_ASSERT(ag_spec->get_option_replication() == 3);
  ag_spec->set_option_blocksize(67108864);
  HT_ASSERT(ag_spec->get_option_blocksize() == 67108864);
  ag_spec->set_option_compressor("bmz --fp-len 20 --offset 5");
  HT_ASSERT(ag_spec->get_option_compressor() == "bmz --fp-len 20 --offset 5");
  ag_spec->set_option_bloom_filter("none");
  HT_ASSERT(ag_spec->get_option_bloom_filter() == "none");
  ag_spec->set_option_in_memory(true);
  HT_ASSERT(ag_spec->get_option_in_memory());
  ag_spec->set_default_max_versions(3);
  HT_ASSERT(ag_spec->get_default_max_versions() == 3);
  ag_spec->set_default_ttl(86400);
  HT_ASSERT(ag_spec->get_default_ttl() == 86400);
  ag_spec->set_default_time_order_desc(true);
  HT_ASSERT(ag_spec->get_default_time_order_desc());
  ag_spec->set_generation(2999);
  HT_ASSERT(ag_spec->get_generation() == 2999);

  ColumnFamilySpec *cf_spec = new ColumnFamilySpec("foo");
  cf_spec->set_access_group("default");
  cf_spec->set_value_index(true);
  cf_spec->set_qualifier_index(true);
  cf_spec->set_option_max_versions(1);
  cf_spec->set_option_time_order_desc(true);
  cf_spec->set_generation(123);
  cf_spec->set_id(42);
  ag_spec->add_column(cf_spec);

  cf_spec = new ColumnFamilySpec("bar");
  cf_spec->set_access_group("default");
  cf_spec->set_qualifier_index(true);
  cf_spec->set_option_ttl(86400);
  cf_spec->set_deleted(true);
  cf_spec->set_generation(234);
  cf_spec->set_id(43);
  ag_spec->add_column(cf_spec);

  cf_spec = new ColumnFamilySpec("baz");
  cf_spec->set_access_group("default");
  cf_spec->set_option_counter(true);
  cf_spec->set_generation(345);
  cf_spec->set_id(44);
  TRY_BAD(ag_spec->add_column(cf_spec));
  cf_spec->set_option_counter(false);
  cf_spec->set_generation(345);
  ag_spec->add_column(cf_spec);

  str = format("HQL: %s\n", ag_spec->render_hql().c_str());
  FileUtils::write(harness.get_log_file_descriptor(), str);
  str = ag_spec->render_xml("", true);
  FileUtils::write(harness.get_log_file_descriptor(), str);

  AccessGroupSpec *after_ag_spec = new AccessGroupSpec();
  after_ag_spec->parse_xml(str.c_str(), str.length());
  HT_ASSERT(*after_ag_spec == *ag_spec);
  string str_after = after_ag_spec->render_xml("", true);
  HT_ASSERT(str == str_after);

  delete ag_spec;
  delete after_ag_spec;

  if (!golden)
    harness.validate_and_exit("AccessGroupSpec_test.golden");

  harness.regenerate_golden_file("AccessGroupSpec_test.golden");

#if defined(TCMALLOC)
  }
  if (!heap_checker.NoLeaks()) assert(!"heap memory leak");
#endif

  return 0;
}
