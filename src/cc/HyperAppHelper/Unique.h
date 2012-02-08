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

#ifndef HYPERAPPHELPER_UNIQUE_H
#define HYPERAPPHELPER_UNIQUE_H

#include "Common/Compat.h"
#include "Common/String.h"
#include "Hypertable/Lib/Namespace.h"
#include "Hypertable/Lib/Table.h"
#include "Hypertable/Lib/Cell.h"

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/lexical_cast.hpp>

namespace Hypertable { namespace HyperAppHelper {

  /**
   * Generates a new GUID
   *
   * GUIDs are globally unique. The generated string is 36 bytes long and 
   * has a format similar to "9cf7da31-307a-4bef-b65e-19fb05aa57d8".
   */
  inline String generate_guid() {
    boost::uuids::random_generator gen;
    boost::uuids::uuid u(gen());
    return boost::lexical_cast<String>(u);
  }

  /**
   * Inserts a unique value into a table
   *
   * This function inserts a unique value into a table. The table must be
   * created with TIME_ORDER DESC, MAX_VERSIONS 1 (although the latter is 
   * optional). 
   *
   * If the value is empty then a new GUID will be assigned 
   * (using @a generate_guid).
   *
   * If the table was not created with TIME_ORDER DESC an exception will be
   * thrown.
   *
   * Unique values are just like any other values. They can be deleted with
   * the regular mutator interface and queried with a scanner.
   *
   * @param table The table pointer
   * @param key The KeySpec object with Row, Column and Column Family
   * @param guid The unique string; will be filled with a new GUID if 
   *        it's empty
   *
   * @sa generate_guid
   */
  extern void create_cell_unique(const TablePtr &table, const KeySpec &key,
                String &guid);

}} // namespace HyperAppHelper, HyperTable

#endif // HYPERAPPHELPER_UNIQUE_H

