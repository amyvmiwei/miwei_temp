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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#ifndef HYPERTABLE_RESULT_H
#define HYPERTABLE_RESULT_H

#include "Common/ReferenceCount.h"
#include "ScanCells.h"

namespace Hypertable {

  class TableMutatorAsync;
  class TableScannerAsync;

  class Result: public ReferenceCount {
    public:

      Result(TableScannerAsync *scanner, ScanCellsPtr &cells);
      Result(TableScannerAsync *scanner, int error,
             const String &error_msg);
      Result(TableMutatorAsync *);
      Result(TableMutatorAsync *, int error, FailedMutations &failed_mutations);

      bool is_error() const { return m_iserror; }
      bool is_scan() const { return m_isscan; }
      bool is_update() const { return !m_isscan; }
      TableScannerAsync *get_scanner();
      TableMutatorAsync *get_mutator();
      void get_cells(Cells &cells);
      void get_error(int &error, String &m_error_msg);
      FailedMutations& get_failed_mutations();
      void get_failed_cells(Cells &cells);
      size_t memory_used() {
        if (m_isscan)
          return (m_cells ? m_cells->memory_used() : 0);
        else
          return (m_iserror ? m_failed_cells.memory_used() : 0);
      }

    private:
      TableScannerAsync *m_scanner;
      TableMutatorAsync *m_mutator;
      ScanCellsPtr m_cells;
      int m_error;
      String m_error_msg;
      bool m_isscan;
      bool m_iserror;
      CellsBuilder m_failed_cells;
      FailedMutations m_failed_mutations;
  };
  typedef intrusive_ptr<Result> ResultPtr;
}

#endif // HYPERTABLE_FUTURE_H

