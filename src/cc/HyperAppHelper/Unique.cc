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
#include "Unique.h"
#include "Hypertable/Lib/Schema.h"
#include "Hypertable/Lib/TableMutator.h"
#include "Hypertable/Lib/TableScanner.h"

namespace Hypertable { namespace HyperAppHelper {

void 
create_cell_unique(const TablePtr &table, const KeySpec &key,
                String &guid)
{
  Schema::ColumnFamily *cf;

  // check the schema - if TIME_ORDER is not descending then this function
  // will not work
  cf=table->schema()->get_column_family(key.column_family);
  if (!cf)
    HT_THROW(Error::FAILED_EXPECTATION, 
            "Unknown column family");
  if (!cf->time_order_desc)
    HT_THROW(Error::BAD_SCAN_SPEC, 
            "Column family is not chronological (use TIME_ORDER DESC)");

  // if the cell has no value: assign a new guid
  if (guid.empty())
    guid=generate_guid();

  // now insert the cell with a regular mutator
  TableMutator *mutator=table->create_mutator();
  mutator->set(key, guid);
  mutator->flush();
  delete mutator;

  // and check if it was really inserted or if another client was faster
  ScanSpecBuilder ssb;
  ssb.add_row((const char *)key.row);
  ssb.add_column(key.column_family);
  ssb.set_cell_limit(1);
  TableScanner *scanner=table->create_scanner(ssb.get());
  Cell c;
  if (!scanner->next(c)) {
    delete scanner;
    HT_THROW(Error::FAILED_EXPECTATION, 
            "Inserted GUID was not found");
  }
  if (c.value_len!=guid.size() || memcmp(c.value, guid.c_str(), c.value_len)) {
    delete scanner;
    HT_THROW(Error::ALREADY_EXISTS, 
            "The inserted cell already exists and is not unique");
  }
  delete scanner;
}

}} // namespace HyperAppHelper, HyperTable
