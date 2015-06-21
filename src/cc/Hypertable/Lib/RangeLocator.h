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

#ifndef Hypertable_Lib_RangeLocator_h
#define Hypertable_Lib_RangeLocator_h

#include <Hypertable/Lib/Key.h>
#include <Hypertable/Lib/LocationCache.h>
#include <Hypertable/Lib/RangeLocationInfo.h>
#include <Hypertable/Lib/RangeServer/Client.h>
#include <Hypertable/Lib/ScanBlock.h>
#include <Hypertable/Lib/Schema.h>
#include <Hypertable/Lib/TableIdentifier.h>

#include <Hyperspace/Session.h>

#include <AsyncComm/ConnectionManager.h>

#include <Common/Error.h>
#include <Common/Timer.h>
#include <Common/Properties.h>

#include <deque>
#include <memory>
#include <mutex>
#include <vector>

namespace Hypertable {

  class RangeLocator;
  class RangeLocatorHyperspaceSessionCallback: public Hyperspace::SessionCallback {
  public:
    RangeLocatorHyperspaceSessionCallback(){}
    ~RangeLocatorHyperspaceSessionCallback() {}
    void safe() {}
    void expired() {}
    void jeopardy() {}
    void disconnected();
    void reconnected();

  private:
    friend class RangeLocator;
    RangeLocator *m_rangelocator;
  };

  /** Locates containing range given a key.  This class does the METADATA range
   * searching to find the location of the range that contains a row key.
   */
  class RangeLocator {

  public:

    /** Constructor.  Loads the METADATA schema and the root range address from
     * Hyperspace. Installs a RootFileHandler to handle root range location
     * changes.
     *
     * @param cfg reference to config properties
     * @param conn_mgr smart pointer to connection manager
     * @param hyperspace smart pointer to Hyperspace session
     * @param timeout_ms timeout in milliseconds
     */
    RangeLocator(PropertiesPtr &cfg, ConnectionManagerPtr &conn_mgr,
                 Hyperspace::SessionPtr &hyperspace, uint32_t timeout_ms);

    /** Destructor.  Closes the root file in Hyperspace */
    ~RangeLocator();

    /** Locates the range that contains the given row key.
     *
     * @param table pointer to table identifier structure
     * @param row_key row key to locate
     * @param range_loc_infop address of RangeLocationInfo to hold result
     * @param timer reference to timer object
     * @param hard don't consult cache
     */
    void find_loop(const TableIdentifier *table, const char *row_key,
                   RangeLocationInfo *range_loc_infop, Timer &timer, bool hard);

    /** Locates the range that contains the given row key.
     *
     * @param table pointer to table identifier structure
     * @param row_key row key to locate
     * @param range_loc_infop address of RangeLocationInfo to hold result
     * @param timer reference to timer object
     * @param hard don't consult cache
     * @return Error::OK on success or error code on failure
     */
    int find(const TableIdentifier *table, const char *row_key,
             RangeLocationInfo *range_loc_infop, Timer &timer, bool hard);

    /**
     * Invalidates the cached entry for the given row key
     *
     * @param table pointer to table identifier structure
     * @param row_key row key to invalidate
     * @return true if invalidated, false if not cached
     */
    bool invalidate(const TableIdentifier *table, const char *row_key) {
      if (table->is_metadata() && (row_key == 0 || strcmp(row_key, Key::END_ROOT_ROW) <= 0))
        m_root_stale = true;
      return m_cache->invalidate(table->id, row_key);
    }

    void invalidate_host(const std::string &hostname) {
      std::lock_guard<std::mutex> lock(m_mutex);
      CommAddress addr;
      addr.set_proxy(hostname);
      if (addr == m_root_range_info.addr)
        m_root_stale = true;
      m_cache->invalidate_host(hostname);
    }

    /** Sets the "root stale" flag.  Causes methods to reread the root range
     * location before doing METADATA scans.
     */
    void set_root_stale() { m_root_stale=true; }

    /**
     * Returns the location cache
     */
    LocationCachePtr location_cache() {
      return m_cache;
    }

    /**
     * Clears the error history
     */
    void clear_error_history() {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_last_errors.clear();
    }

    /**
     * Displays the error history
     */
    void dump_error_history() {
      std::lock_guard<std::mutex> lock(m_mutex);
      for (auto &e : m_last_errors)
        HT_ERROR_OUT << e << HT_END;
      m_last_errors.clear();
    }

  private:
    friend class RangeLocatorHyperspaceSessionCallback;

    void initialize(Timer &timer);
    void hyperspace_disconnected();
    void hyperspace_reconnected();
    int process_metadata_scanblocks(std::vector<ScanBlock> &scan_blocks, Timer &timer);
    int read_root_location(Timer &timer);
    void initialize();
    int connect(CommAddress &addr, Timer &timer);

    std::mutex m_mutex;
    ConnectionManagerPtr   m_conn_manager;
    Hyperspace::SessionPtr m_hyperspace;
    LocationCachePtr       m_cache;
    uint64_t               m_root_file_handle;
    Hyperspace::HandleCallbackPtr m_root_handler;
    bool                   m_root_stale;
    RangeLocationInfo      m_root_range_info;
    Lib::RangeServer::Client m_range_server;
    uint8_t                m_startrow_cid;
    uint8_t                m_location_cid;
    TableIdentifier        m_metadata_table;
    std::deque<Exception>  m_last_errors;
    bool                   m_hyperspace_init;
    bool                   m_hyperspace_connected;
    std::mutex m_hyperspace_mutex;
    uint32_t               m_timeout_ms;
    RangeLocatorHyperspaceSessionCallback m_hyperspace_session_callback;
    std::string                 m_toplevel_dir;
    uint32_t               m_metadata_readahead_count;
    uint32_t               m_max_error_queue_length;
    uint32_t               m_metadata_retry_interval;
    uint32_t               m_root_metadata_retry_interval;
  };

  /// Smart pointer to RangeLocator
  typedef std::shared_ptr<RangeLocator> RangeLocatorPtr;

}

#endif // Hypertable_Lib_RangeLocator_h
