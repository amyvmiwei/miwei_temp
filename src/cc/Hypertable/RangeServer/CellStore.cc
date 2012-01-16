/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
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
#include "CellStore.h"
#include "KeyDecompressorNone.h"

using namespace Hypertable;

const char CellStore::DATA_BLOCK_MAGIC[10]           =
    { 'D','a','t','a','-','-','-','-','-','-' };
const char CellStore::INDEX_FIXED_BLOCK_MAGIC[10]    =
    { 'I','d','x','F','i','x','-','-','-','-' };
const char CellStore::INDEX_VARIABLE_BLOCK_MAGIC[10] =
    { 'I','d','x','V','a','r','-','-','-','-' };

KeyDecompressor *CellStore::create_key_decompressor() {
  return new KeyDecompressorNone();
}

void CellStore::set_replaced_files(const std::vector<String> &old_files) {
  m_replaced_files = old_files;
}

const std::vector<String> &CellStore::get_replaced_files() {
  return m_replaced_files;
}
