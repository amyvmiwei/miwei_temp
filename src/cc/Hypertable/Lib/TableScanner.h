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

#ifndef HYPERTABLE_TABLESCANNERSYNC_H
#define HYPERTABLE_TABLESCANNERSYNC_H

#include <list>
#include "Common/ReferenceCount.h"
#include "TableScannerQueue.h"
#include "TableScannerAsync.h"
#include "TableCallback.h"
#include "ScanCells.h"

namespace Hypertable {

  /**
   */
  class TableScanner : public ReferenceCount {

  public:

    /**
     * Constructs a TableScanner object.
     *
     * @param comm pointer to the Comm layer
     * @param table pointer to the table object
     * @param range_locator smart pointer to range locator
     * @param scan_spec reference to scan specification object
     * @param timeout_ms maximum time in milliseconds to allow scanner
     *        methods to execute before throwing an exception
     */
    TableScanner(Comm *comm, Table *table,  RangeLocatorPtr &range_locator,
                 const ScanSpec &scan_spec, uint32_t timeout_ms);

    /**
     * Cancel asynchronous scanner and keep dealing with RangeServer responses
     * till async scanner is done
     */
    ~TableScanner() {
      try {
        m_scanner->cancel();
        if (!m_scanner->is_complete()) {
          ScanCellsPtr cells;
          int error=Error::OK;
          String error_msg;
          while (!m_scanner->is_complete())
            m_queue->next_result(cells, &error, error_msg);
        }
      }
      catch(Exception &e) {
        HT_ERROR_OUT << e << HT_END;
      }
    }

    /**
     * Get the next cell.
     *
     * @param cell The cell object to contain the result
     * @return true for success
     */
    bool next(Cell &cell);

    /**
     * Unget one cell.
     *
     * Only one cell that's previously obtained from #next can be unget. Mostly
     * designed to provide one cell look-ahead for downstream wrapper to
     * implement next_row.
     *
     * @param cell the cell object to unget
     * @throws exception if unget is called twice without intervening next
     */
    void unget(const Cell &cell);

  private:

    friend class TableCallback;
    /**
     * Callback method for successful scan
     *
     * @param cells Vector of returned cells
     */
    void scan_ok(ScanCellsPtr &cells);

    /**
     * Callback method for scan errors
     *
     * @param error
     * @param error_msg
     */
    void scan_error(int error, const String &error_msg);

    TableCallback m_callback;
    TableScannerQueuePtr m_queue;
    TableScannerAsyncPtr m_scanner;
    ScanCellsPtr m_cur_cells;
    size_t m_cur_cells_index;
    size_t m_cur_cells_size;
    int m_error;
    String m_error_msg;
    bool m_eos;
    Cell m_ungot;
  };
  typedef intrusive_ptr<TableScanner> TableScannerPtr;

  void copy(TableScanner &scanner, CellsBuilder &b);
  inline void copy(TableScannerPtr &p, CellsBuilder &v) { copy(*p.get(), v); }
} // namesapce Hypertable

#endif // HYPERTABLE_TABLESCANNER_H
