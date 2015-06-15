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

#ifndef Hypertable_Lib_intervalScannerAsync_h
#define Hypertable_Lib_intervalScannerAsync_h

#include <Hypertable/Lib/ProfileDataScanner.h>
#include <Hypertable/Lib/RangeLocator.h>
#include <Hypertable/Lib/RangeServer/Client.h>
#include <Hypertable/Lib/ScanBlock.h>
#include <Hypertable/Lib/ScanCells.h>
#include <Hypertable/Lib/TableIdentifier.h>
#include <Hypertable/Lib/TableScannerDispatchHandler.h>

#include <Common/Properties.h>
#include <Common/StringExt.h>

#include <memory>
#include <mutex>

namespace Hypertable {

  using namespace Lib;

  class Table;
  class TableScannerAsync;

  class IntervalScannerAsync {

  public:
    /**
     * Constructs a IntervalScannerAsync object.
     *
     * @param comm pointer to the Comm layer
     * @param app_queue Application Queue pointer
     * @param table ponter to the table
     * @param range_locator smart pointer to range locator
     * @param scan_spec reference to scan specification object
     * @param timeout_ms maximum time in milliseconds to allow scanner
     *        methods to execute before throwing an exception
     * @param current is this scanner the current scanner being used
     * @param scanner pointer to table scanner
     * @param id scanner id
     */
    IntervalScannerAsync(Comm *comm, ApplicationQueueInterfacePtr &app_queue, Table *table,
                         RangeLocatorPtr &range_locator,
                         const ScanSpec &scan_spec, uint32_t timeout_ms,
                         bool current, TableScannerAsync *scanner, int id);

    virtual ~IntervalScannerAsync();

    bool abort(bool is_create);
    // if we can't retry then abort scanner
    bool retry_or_abort(bool refresh, bool hard, bool is_create, 
            bool *move_to_next, int last_error);
    bool handle_result(bool *show_results, ScanCellsPtr &cells, EventPtr &event, bool is_create);
    bool set_current(bool *show_results, ScanCellsPtr &cells, bool abort);
    inline bool has_outstanding_requests() { return m_create_outstanding || m_fetch_outstanding; }
    int64_t bytes_scanned() { return m_bytes_scanned; }
    bool is_destroyed_scanner(bool is_create);

    /// Returns reference to profile data.
    /// @return Reference to profile data
    ProfileDataScanner &profile_data() { return m_profile_data; }

  private:
    void reset_outstanding_status(bool is_create, bool reset_timer);
    void readahead();
    void init(const ScanSpec &);
    void find_range_and_start_scan(const char *row_key, bool hard=false);
    void set_result(EventPtr &event, ScanCellsPtr &cells, bool is_create=false);
    void load_result(ScanCellsPtr &cells);
    void set_range_spec(DynamicBuffer &dbuf, RangeSpec &range);
    void restart_scan(bool refresh=false);

    Table              *m_table;
    SchemaPtr           m_schema;
    RangeLocatorPtr     m_range_locator;
    LocationCachePtr    m_loc_cache;
    ScanSpecBuilder     m_scan_spec_builder;
    ScanLimitState      m_scan_limit_state;
    Lib::RangeServer::Client m_range_server;
    TableIdentifierManaged m_table_identifier;
    /// Accumulated profile data
    ProfileDataScanner m_profile_data;
    bool                m_eos;
    std::string              m_create_scanner_row;
    RangeLocationInfo   m_range_info;
    RangeLocationInfo   m_next_range_info;
    bool                m_fetch_outstanding;
    bool                m_create_outstanding;
    EventPtr            m_create_event;
    std::string              m_start_row;
    std::string              m_end_row;
    bool                m_end_inclusive;
    uint32_t            m_timeout_ms;
    bool                m_current;
    int64_t             m_bytes_scanned;
    CstrSet             m_rowset;
    TableScannerDispatchHandler m_create_handler;
    TableScannerDispatchHandler m_fetch_handler;
    Timer               m_create_timer;
    Timer               m_fetch_timer;
    bool                m_cur_scanner_finished;
    int                 m_cur_scanner_id;
    int                 m_state;
    Key                 m_last_key;
    DynamicBuffer       m_last_key_buf;
    bool                m_create_event_saved;
    bool                m_invalid_scanner_id_ok;
    bool m_defer_readahead {};
  };

  /// Smart pointer to IntervalScannerAsync
  typedef std::shared_ptr<IntervalScannerAsync> IntervalScannerAsyncPtr;

}

#endif // Hypertable_Lib_intervalScannerAsync_h
