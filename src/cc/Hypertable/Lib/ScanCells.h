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

#ifndef HYPERTABLE_SCANCELLS_H
#define HYPERTABLE_SCANCELLS_H

#include "Common/Compat.h"

#include <vector>

#include "Common/StringExt.h"
#include "Common/ReferenceCount.h"

#include "Cells.h"
#include "ScanBlock.h"
#include "ScanLimitState.h"
#include "Schema.h"

namespace Hypertable {

using namespace std;
class IntervalScannerAsync;

/**
 * This class takes allows vector access to a set of cells contained in an EventPtr without
 * any copying.
 */
class ScanCells : public ReferenceCount {

public:
  ScanCells() : m_eos(false){}

  void get(Cells &cells) {
    if (m_cells) {
      m_cells->get(cells);
    }
    else {
      cells.clear();
    }
  }
  void get_cell_unchecked(Cell &cc, size_t ii) { m_cells->get_cell(cc, ii); }
  void set_eos(bool eos = true) { m_eos = eos; }
  bool get_eos() const { return m_eos; }
  size_t size() const {
    if (m_cells)
      return m_cells->size();
    else
      return 0;
  }

  bool empty() const {
    return size() == 0;
  }

  size_t memory_used() const {
    size_t mem_used=0;
    foreach_ht(const ScanBlockPtr &v, m_scanblocks) {
      mem_used += v->memory_used();
    }
    return mem_used;
  }

protected:

  friend class IntervalScannerAsync;
  friend class IndexScannerCallback;

  /**
   * @param event the event that contains the scan results
   * @param scanner_id scanner_id for the scanner
   * @return true if this if this event has the eos bit set
   */
  bool add(EventPtr &event, int *scanner_id);

  /**
   * adds a new cell to the internal cell buffer
   * this is an internal method required by IndexScannerCallback
   */
  void add(Cell &cell, bool own = true);

  /**
   * @param schema is the schema for the table being scanned
   * @param end_row the end_row of the scan for which we got these results
   * @param end_inclusive is the end_row included in the scan
   * @param limit_state Pointer to ScanLimitState
   * @param rowset Reference to set of rows to be selected
   * @param bytes_scanned number of bytes read
   * @param lastkey Return pointer to last key in block
   * @return true if scan has reached end
   */
  bool load(SchemaPtr &schema, const String &end_row, bool end_inclusive,
	    ScanLimitState *limit_state, CstrSet &rowset,
	    int64_t *bytes_scanned, Key *lastkey);

  /**
   * get number of rows that were skipped because of an OFFSET predicate
   */
  int get_skipped_rows() {
    foreach_ht(const ScanBlockPtr &v, m_scanblocks) {
      if (v->get_skipped_rows())
        return (v->get_skipped_rows());
    }
    return 0;
  }

  /**
   * get number of cells that were skipped because of a CELL_OFFSET predicate
   */
  int get_skipped_cells() {
    foreach_ht(const ScanBlockPtr &v, m_scanblocks) {
      if (v->get_skipped_cells())
        return (v->get_skipped_cells());
    }
    return 0;
  }

  vector<ScanBlockPtr> m_scanblocks;
  CellsBuilderPtr m_cells;
  bool m_eos;
}; // ScanCells

typedef intrusive_ptr<ScanCells> ScanCellsPtr;

} // namespace Hypertable

#endif // HYPERTABLE_SCANCELLS_H
