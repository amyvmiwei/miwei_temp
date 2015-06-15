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

#include "Common/Compat.h"
#include "Common/DynamicBuffer.h"
#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/StringExt.h"

#include <cstdlib>
#include "Common/ScopeGuard.h"
#include "Hyperspace/Session.h"

#include "TableInfo.h"

using namespace Hyperspace;

namespace Tools {
namespace client {
namespace rangeserver {

  TableInfo::TableInfo(const String &toplevel_dir, const String &table_id)
    : m_toplevel_dir(toplevel_dir) {
    m_table.set_id(table_id);
  }

  void TableInfo::load(Hyperspace::SessionPtr &hyperspace) {
    String table_file = m_toplevel_dir + "/tables/" + m_table.id;
    DynamicBuffer valbuf(0);

    hyperspace->attr_get(table_file, "schema", valbuf);

    m_schema.reset( Schema::new_instance((const char *)valbuf.base) );

    m_table.generation = m_schema->get_generation();
  }

}}}
