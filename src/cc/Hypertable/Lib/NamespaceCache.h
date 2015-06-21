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

#ifndef Hypertable_Lib_NamespaceCache_h
#define Hypertable_Lib_NamespaceCache_h

#include <Hypertable/Lib/TableCache.h>
#include <Hypertable/Lib/Schema.h>
#include <Hypertable/Lib/RangeLocator.h>
#include <Hypertable/Lib/Namespace.h>

#include <AsyncComm/ApplicationQueueInterface.h>

#include <Common/String.h>

#include <memory>
#include <mutex>
#include <unordered_map>

namespace Hypertable {

  class Client;

  class NamespaceCache {
  public:

    NamespaceCache(PropertiesPtr &props, RangeLocatorPtr &range_locator,
                   ConnectionManagerPtr &conn_manager, Hyperspace::SessionPtr &hyperspace,
                   ApplicationQueueInterfacePtr &app_queue, NameIdMapperPtr &namemap,
                   Lib::Master::ClientPtr &master_client, TableCachePtr &table_cache,
                   uint32_t default_timeout_ms, Client *client);

    /**
     * @param name namespace name
     * @return NULL if the namespace doesn't exist, ow return a pointer to the Namespace
     */
    NamespacePtr get(const std::string &name);

    /**
     * @param name
     * @return false if entry is not in cache
     */
    bool remove(const std::string &name);

  private:
    typedef std::unordered_map<String, NamespacePtr> NamespaceMap;

    PropertiesPtr           m_props;
    RangeLocatorPtr         m_range_locator;
    Comm                   *m_comm;
    ConnectionManagerPtr    m_conn_manager;
    Hyperspace::SessionPtr  m_hyperspace;
    ApplicationQueueInterfacePtr     m_app_queue;
    NameIdMapperPtr         m_namemap;
    Lib::Master::ClientPtr m_master_client;
    TableCachePtr           m_table_cache;
    uint32_t                m_timeout_ms;
    std::mutex m_mutex;
    NamespaceMap            m_namespace_map;
    Client                 *m_client;
  };

  /// Smart pointer to NamespaceCache
  typedef std::shared_ptr<NamespaceCache> NamespaceCachePtr;

}


#endif // Hypertable_Lib_NamespaceCache_h
