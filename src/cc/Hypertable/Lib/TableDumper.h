/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License.
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

#ifndef HYPERTABLE_TABLEDUMPER_H
#define HYPERTABLE_TABLEDUMPER_H

#include "Common/ReferenceCount.h"

#include "Cells.h"
#include "Namespace.h"
#include "ScanSpec.h"
#include "TableSplit.h"

namespace Hypertable {

  class TableDumper : public ReferenceCount {

  public:
    /**
     * Constructs a TableDumper object.
     *
     * @param ns pointer to namespace object
     * @param name table name
     * @param scan_spec scan specification
     * @param target_node_count target node count
     */
    TableDumper(NamespacePtr &ns, const String &name, ScanSpec &scan_spec,
		size_t target_node_count=20);

    /**
     * Get the next cell.
     *
     * @param cell The cell object to contain the result
     * @return true for success
     */
    bool next(Cell &cell);

  private:
    ScanSpec  m_scan_spec;
    TableSplitsContainer m_splits;
    TablePtr m_table;
    std::vector<uint32_t> m_ordering;
    uint32_t  m_next;
    std::list<TableScannerPtr> m_scanners;
    std::list<TableScannerPtr>::iterator m_scanner_iter;
    bool      m_eod;
    int64_t   m_rows_seen;
  };

  typedef intrusive_ptr<TableDumper> TableDumperPtr;

  void copy(TableDumper &, CellsBuilder &);
  inline void copy(TableDumperPtr &p, CellsBuilder &v) { copy(*p.get(), v); }

} // namespace Hypertable

#endif // HYPERTABLE_TABLEDUMPER_H
