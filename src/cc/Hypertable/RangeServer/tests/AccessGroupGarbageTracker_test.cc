/* -*- c++ -*-
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
#include "Common/Config.h"
#include "Common/Init.h"

#include <iostream>

#include "../AccessGroupGarbageTracker.h"
#include "../Global.h"

using namespace Hypertable;
using namespace Config;

#if 0
namespace {

  const char *schema_str =
  "<Schema>\n"
  "  <AccessGroup name=\"default\">\n"
  "    <ColumnFamily>\n"
  "      <Name>tag</Name>\n"
  "    </ColumnFamily>\n"
  "  </AccessGroup>\n"
  "</Schema>";


  const char *schema2_str =
  "<Schema>\n"
  "  <AccessGroup name=\"default\">\n"
  "    <ColumnFamily>\n"
  "      <MaxVersions>1</MaxVersions>\n"   
  "      <Name>tag</Name>\n"
  "    </ColumnFamily>\n"
  "  </AccessGroup>\n"
  "</Schema>";

  const char *schema3_str =
  "<Schema>\n"
  "  <AccessGroup name=\"default\">\n"
  "    <ColumnFamily>\n"
  "      <ttl>3600</ttl>\n"
  "      <Name>tag</Name>\n"
  "    </ColumnFamily>\n"
  "  </AccessGroup>\n"
  "</Schema>";

}
#endif


int main(int argc, char **argv) {
#if 0
  init_with_policy<DefaultPolicy>(argc, argv);

  Global::access_group_garbage_compaction_threshold 
    = properties->get_i32("Hypertable.RangeServer.AccessGroup.GarbageThreshold.Percentage");

  {

    SchemaPtr schema = Schema::new_instance(schema_str, strlen(schema_str));
    if (!schema->is_valid()) {
      HT_ERRORF("Schema Parse Error: %s", schema->get_error_string());
      exit(1);
    }
    Schema::AccessGroup *ag = schema->get_access_group("default");

    AccessGroupGarbageTracker tracker(properties, ag);

    HT_ASSERT(!tracker.check_needed(time(0)));

    tracker.accumulate(1, 0);
    HT_ASSERT(!tracker.check_needed(time(0)));

    tracker.clear();

    int64_t split_size = properties->get_i64("Hypertable.RangeServer.Range.SplitSize");

    int64_t amount = split_size / 5;
    tracker.accumulate(amount, 0);
    HT_ASSERT(!tracker.check_needed(time(0)));

    tracker.accumulate(0, 1);
    HT_ASSERT(tracker.check_needed(time(0)));

    tracker.clear();
    tracker.accumulate(split_size/20, 1);
    HT_ASSERT(!tracker.check_needed(time(0)));
    tracker.accumulate(split_size/10, 0);
    HT_ASSERT(tracker.check_needed(time(0)));

    tracker.clear();

    schema = Schema::new_instance(schema2_str, strlen(schema2_str));
    if (!schema->is_valid()) {
      HT_ERRORF("Schema Parse Error: %s", schema->get_error_string());
      exit(1);
    }
    ag = schema->get_access_group("default");

    tracker.update_schema(ag);
    HT_ASSERT(!tracker.check_needed(time(0)));

    tracker.accumulate(amount, 0);
    HT_ASSERT(tracker.check_needed(time(0)));

    tracker.clear();
    tracker.accumulate(amount, 0);
    HT_ASSERT(tracker.set_garbage_stats(time(0), 1000000.0, 900000.0));
    
    tracker.clear();
    tracker.accumulate(amount, 0);
    HT_ASSERT(!tracker.set_garbage_stats(time(0), 1000000.0, 50000.0));

    tracker.clear();
    tracker.accumulate(amount, 0);
    HT_ASSERT(tracker.set_garbage_stats(time(0), 1000000.0, 400000.0));

    tracker.clear();
    tracker.accumulate(amount, 0);
    HT_ASSERT(tracker.set_garbage_stats(time(0), 1000000.0, 300000.0));

    tracker.clear();
    tracker.accumulate(amount, 0);
    HT_ASSERT(tracker.set_garbage_stats(time(0), 1000000.0, 500000.0));

    tracker.clear();
    tracker.accumulate(2*amount, 0);
    HT_ASSERT(tracker.set_garbage_stats(time(0), 1000000.0, 500000.0));

    tracker.clear();
    tracker.accumulate(amount, 0);
    HT_ASSERT(!tracker.set_garbage_stats(time(0), 1000000.0, 100000.0));

    tracker.clear();

    schema = Schema::new_instance(schema3_str, strlen(schema3_str));
    if (!schema->is_valid()) {
      HT_ERRORF("Schema Parse Error: %s", schema->get_error_string());
      exit(1);
    }
    ag = schema->get_access_group("default");

    tracker.update_schema(ag);
    HT_ASSERT(!tracker.check_needed(time(0)));

    tracker.clear();
    time_t base_time = time(0);
    time_t now;

    now = base_time + 2;
    tracker.accumulate_expirable(amount/4);
    HT_ASSERT(!tracker.check_needed(now, 0, 0));

    now = base_time + 360;
    HT_ASSERT(tracker.check_needed(now, amount/2, 0));

    now = base_time + 3;
    tracker.accumulate_expirable(amount/2);
    HT_ASSERT(!tracker.check_needed(now, 0, 0));

    now = base_time + 360;
    HT_ASSERT(tracker.check_needed(now, 0, 0));

    now = base_time + 359;
    HT_ASSERT(!tracker.check_needed(now, 0, 0));

  }
#endif
  return 0;
}

