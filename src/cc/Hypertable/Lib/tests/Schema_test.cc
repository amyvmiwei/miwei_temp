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

#include <Hypertable/Lib/Schema.h>

#include <Common/FileUtils.h>
#include <Common/Logger.h>
#include <Common/TestHarness.h>
#include <Common/Time.h>
#include <Common/Usage.h>

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
    "usage: Schema_test [--golden]",
    "",
    "Validates Schema class.  If --golden is supplied, a new golden",
    "output file 'Schema_test.golden' will be generated",
    0
  };
}


int main(int argc, char **argv) {
  bool golden = false;
  TestHarness harness("Schema_test");
  SchemaPtr schema;

  if (argc > 1) {
    if (!strcmp(argv[1], "--golden"))
      golden = true;
    else
      Usage::dump_and_exit(usage);
  }

  size_t begin {};
  size_t end {};
  string test_schemas = FileUtils::file_to_string("test-schemas.xml");

  HT_ASSERT(!test_schemas.empty());

  int testi = 0;
  while ((begin = test_schemas.find("<Schema", end)) != string::npos) {
    FileUtils::write(harness.get_log_file_descriptor(),
                     format("<!-- %d -->\n", ++testi));
    end = test_schemas.find("</Schema>", begin);
    HT_ASSERT(end != string::npos);
    end += strlen("</Schema>");
    string schema_str(test_schemas, begin, end-begin);
    try {
      schema.reset( Schema::new_instance(schema_str) );
      schema_str = schema->render_xml(true);
      FileUtils::write(harness.get_log_file_descriptor(), schema_str);
    }
    catch (Exception &e) {
      FileUtils::write(harness.get_log_file_descriptor(),
                       format("%s: %s\n", Error::get_text(e.code()), e.what()));
    }
  }

  schema = make_shared<Schema>();
  AccessGroupOptions ag_defaults;
  ag_defaults.set_blocksize(65000);
  schema->set_access_group_defaults(ag_defaults);
  schema->set_group_commit_interval(200);

  AccessGroupSpec *ag_spec = new AccessGroupSpec("default");
  ColumnFamilySpec *cf_spec = new ColumnFamilySpec("actions");
  cf_spec->set_option_ttl(2592000);
  cf_spec->set_option_max_versions(3);
  ag_spec->add_column(cf_spec);
  schema->add_access_group(ag_spec);

  ag_spec = new AccessGroupSpec("meta");
  ag_spec->set_default_max_versions(2);
  cf_spec = new ColumnFamilySpec("language");
  ag_spec->add_column(cf_spec);
  cf_spec = new ColumnFamilySpec("checksum");
  cf_spec->set_option_max_versions(1);
  ag_spec->add_column(cf_spec);
  schema->add_access_group(ag_spec);

  std::string str = schema->render_hql("MyTable");
  FileUtils::write(harness.get_log_file_descriptor(), str);
  FileUtils::write(harness.get_log_file_descriptor(), "\n");

  str = schema->render_xml();
  FileUtils::write(harness.get_log_file_descriptor(), str);

  schema->update_generation(42);
  str = schema->render_xml(true);
  FileUtils::write(harness.get_log_file_descriptor(), str);

  SchemaPtr orig_schema = make_shared<Schema>(*schema);
  cf_spec->set_option_max_versions(2);
  HT_ASSERT(schema->clear_generation_if_changed(*orig_schema));
  str = schema->render_xml(true);
  FileUtils::write(harness.get_log_file_descriptor(), str);

  if (!golden)
    harness.validate_and_exit("Schema_test.golden");

  harness.regenerate_golden_file("Schema_test.golden");

  return 0;
}
