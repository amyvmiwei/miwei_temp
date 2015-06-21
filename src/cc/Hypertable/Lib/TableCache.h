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

#ifndef Hypertable_Lib_TableCache_h
#define Hypertable_Lib_TableCache_h

#include <Hypertable/Lib/RangeLocator.h>
#include <Hypertable/Lib/Schema.h>
#include <Hypertable/Lib/Table.h>

#include <AsyncComm/ApplicationQueueInterface.h>

#include <Common/String.h>

#include <memory>
#include <mutex>
#include <unordered_map>

namespace Hypertable {

  class TableCache : private Hyperspace::SessionCallback {
  public:

    TableCache(PropertiesPtr &, RangeLocatorPtr &, ConnectionManagerPtr &,
               Hyperspace::SessionPtr &, ApplicationQueueInterfacePtr &, 
               NameIdMapperPtr &namemap, uint32_t default_timeout_ms);

    virtual ~TableCache();

    /**
     *
     */
    TablePtr get(const std::string &table_name, int32_t flags);

    TablePtr get_unlocked(const std::string &table_name, int32_t flags);

    /**
     * @param table_name Name of table
     * @param output_schema string representation of Schema object
     * @param with_ids if true return CF ids
     * @return false if table_name is not in cache
     */
    bool get_schema_str(const std::string &table_name, std::string &output_schema, bool with_ids=false);

    /**
     *
     * @param table_name Name of table
     * @param output_schema Schema object
     * @return false if table_name is not in cache
     */
    bool get_schema(const std::string &table_name, SchemaPtr &output_schema);

    /**
     * @param table_name
     * @return false if entry is not in cache
     */
    bool remove(const std::string &table_name);

    void lock() { m_mutex.lock(); }
    void unlock() { m_mutex.unlock(); }

  private:

    virtual void safe() { }
    virtual void expired() { }
    virtual void jeopardy() { }
    virtual void disconnected() { }
    virtual void reconnected();

    typedef std::unordered_map<String, TablePtr> TableMap;

    std::mutex m_mutex;
    PropertiesPtr           m_props;
    RangeLocatorPtr         m_range_locator;
    Comm                   *m_comm;
    ConnectionManagerPtr    m_conn_manager;
    Hyperspace::SessionPtr  m_hyperspace;
    ApplicationQueueInterfacePtr     m_app_queue;
    NameIdMapperPtr         m_namemap;
    uint32_t                m_timeout_ms;
    TableMap                m_table_map;
  };

  /// Smart pointer to TableCache
  typedef std::shared_ptr<TableCache> TableCachePtr;

}

#endif // Hypertable_Lib_TableCache_h
