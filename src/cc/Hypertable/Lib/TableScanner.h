/* -*- c++ -*-
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

#ifndef Hypertable_Lib_TableScanner_h
#define Hypertable_Lib_TableScanner_h

#include <Hypertable/Lib/ClientObject.h>
#include <Hypertable/Lib/TableScannerQueue.h>
#include <Hypertable/Lib/TableScannerAsync.h>
#include <Hypertable/Lib/TableCallback.h>
#include <Hypertable/Lib/ScanCells.h>

#include <list>

namespace Hypertable {

  /// @addtogroup libHypertable
  /// @{

  /** Synchronous table scanner. */
  class TableScanner : public ClientObject {

  public:

    /** Constructor.
     * @param comm Comm layer object
     * @param table Table object
     * @param range_locator Smart pointer to range locator
     * @param scan_spec Scan specification
     * @param timeout_ms Timeout (deadline) milliseconds
     */
    TableScanner(Comm *comm, Table *table,  RangeLocatorPtr &range_locator,
                 const ScanSpec &scan_spec, uint32_t timeout_ms);

    /** Destructor.
     * Cancel asynchronous scanner and keep dealing with RangeServer responses
     * till async scanner is finished.
     */
    virtual ~TableScanner() {
      try {
        m_scanner->cancel();
        if (!m_scanner->is_complete()) {
          ScanCellsPtr cells;
          int error=Error::OK;
          std::string error_msg;
          while (!m_scanner->is_complete())
            m_queue->next_result(cells, &error, error_msg);
        }
      }
      catch(Exception &e) {
        HT_ERROR_OUT << e << HT_END;
      }
    }

    /** Gets the next cell.
     * @param cell The cell object to contain the result
     * @return <i>true</i> on success, <i>false</i> on end of scan
     */
    bool next(Cell &cell);

    /** Ungets one cell.
     * Only one cell that's previously obtained from #next can be unget. Mostly
     * designed to provide one cell look-ahead for downstream wrapper to
     * implement next_row.
     * @param cell the cell object to unget
     * @throws exception if unget is called twice without intervening next
     */
    void unget(const Cell &cell);

    /// Gets profile data.
    /// @param profile_data Reference to profile data object populated by this
    /// method
    void get_profile_data(ProfileDataScanner &profile_data) {
      m_scanner->get_profile_data(profile_data);
    }

  private:

    friend class TableCallback;

    /** Callback for successful scan.
     * @param cells Vector of returned cells
     */
    void scan_ok(ScanCellsPtr &cells);

    /** Callback for scan errors.
     * @param error
     * @param error_msg
     */
    void scan_error(int error, const std::string &error_msg);

    TableCallback m_callback;
    TableScannerQueuePtr m_queue;
    TableScannerAsyncPtr m_scanner;
    ScanCellsPtr m_cur_cells;
    ProfileDataScanner m_profile_data;
    size_t m_cur_cells_index;
    size_t m_cur_cells_size;
    int m_error;
    std::string m_error_msg;
    bool m_eos;
    Cell m_ungot;
  };
  
  /// Smart pointer to TableScanner.
  typedef std::shared_ptr<TableScanner> TableScannerPtr;

  void copy(TableScanner &scanner, CellsBuilder &b);
  inline void copy(TableScannerPtr &p, CellsBuilder &v) { copy(*p.get(), v); }

  /// @}

}

#endif // Hypertable_Lib_TableScanner_h
