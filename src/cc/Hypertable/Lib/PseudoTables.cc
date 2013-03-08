/*
 * Copyright (C) 2007-2013 Hypertable, Inc.
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

/** @file
 * Member definitions for PseudoTables class.
 * This file contains the method and member definitions for PseudoTables, a
 * singleton class that holds Schema objects for the pseudo tables.
 */

#include "Common/Compat.h"

#include <cstring>

#include "PseudoTables.h"

using namespace Hypertable;

PseudoTables *PseudoTables::ms_instance = 0;
Mutex PseudoTables::ms_mutex;

namespace {

  const char *cellstore_index_schema_str = 
    "<Schema generation=\"1\">\n"
    "  <AccessGroup name=\"default\">\n"
    "    <ColumnFamily id=\"1\">\n"
    "      <Generation>1</Generation>\n"
    "      <Name>Size</Name>\n"
    "    </ColumnFamily>\n"
    "    <ColumnFamily id=\"2\">\n"
    "      <Generation>1</Generation>\n"
    "      <Name>CompressedSize</Name>\n"
    "    </ColumnFamily>\n"
    "    <ColumnFamily id=\"3\">\n"
    "      <Generation>1</Generation>\n"
    "      <Name>KeyCount</Name>\n"
    "    </ColumnFamily>\n"
    "  </AccessGroup>\n"
    "</Schema>\n";
}

PseudoTables::PseudoTables() {

  cellstore_index = Schema::new_instance(cellstore_index_schema_str,
                                         strlen(cellstore_index_schema_str));
}

PseudoTables::~PseudoTables() {
}
