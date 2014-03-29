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

#ifndef Hypertable_Lib_IndexTables_h
#define Hypertable_Lib_IndexTables_h

#include <Hypertable/Lib/Key.h>
#include <Hypertable/Lib/TableMutatorAsync.h>

namespace Hypertable { namespace IndexTables {

  void add(const Key &key, uint8_t flag,
           const void *value, uint32_t value_len,
           TableMutatorAsync *value_index_mutator,
           TableMutatorAsync *qualifier_index_mutator);

}}

#endif // Hypertable_Lib_IndexTables_h
