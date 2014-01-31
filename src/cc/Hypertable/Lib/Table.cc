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
#include <cstring>

#include <boost/algorithm/string.hpp>

#include "Common/String.h"
#include "Common/DynamicBuffer.h"
#include "Common/Error.h"
#include "Common/Logger.h"

#include "AsyncComm/ApplicationQueue.h"

#include "Hyperspace/HandleCallback.h"
#include "Hyperspace/Session.h"

#include "Table.h"
#include "TableScanner.h"
#include "TableMutator.h"
#include "TableMutatorShared.h"
#include "TableMutatorAsync.h"
#include "ScanSpec.h"

using namespace Hypertable;
using namespace Hyperspace;


Table::Table(PropertiesPtr &props, ConnectionManagerPtr &conn_manager,
             Hyperspace::SessionPtr &hyperspace, NameIdMapperPtr &namemap,
             const String &name, int32_t flags)
  : m_props(props), m_comm(conn_manager->get_comm()),
    m_conn_manager(conn_manager), m_hyperspace(hyperspace), m_namemap(namemap),
    m_name(name), m_flags(flags), m_stale(true), m_namespace(0) {
  m_timeout_ms = props->get_i32("Hypertable.Request.Timeout");
  initialize();

  m_range_locator = new RangeLocator(props, m_conn_manager, m_hyperspace,
                                     m_timeout_ms);

  m_app_queue = new ApplicationQueue(props->get_i32("Hypertable.Client.Workers"));
}


Table::Table(PropertiesPtr &props, RangeLocatorPtr &range_locator,
             ConnectionManagerPtr &conn_manager, 
             Hyperspace::SessionPtr &hyperspace, ApplicationQueueInterfacePtr &app_queue,
             NameIdMapperPtr &namemap, const String &name, 
             int32_t flags, uint32_t timeout_ms)
  : m_props(props), m_comm(conn_manager->get_comm()),
    m_conn_manager(conn_manager), m_hyperspace(hyperspace),
    m_range_locator(range_locator), m_app_queue(app_queue), m_namemap(namemap),
    m_name(name), m_flags(flags), m_timeout_ms(timeout_ms), m_stale(true),
    m_namespace(0) {
  initialize();
}


void Table::initialize() {
  String tablefile;
  DynamicBuffer value_buf(0);
  String errmsg;
  String table_id;
  bool is_index = false;

  {
    size_t pos = m_name.find_last_of('/');
    if (pos == std::string::npos)
      pos = 0;
    else
      pos++;
    is_index = m_name[pos] == '^';
  }

  m_toplevel_dir = m_props->get_str("Hypertable.Directory");
  boost::trim_if(m_toplevel_dir, boost::is_any_of("/"));
  m_toplevel_dir = String("/") + m_toplevel_dir;

  m_scanner_queue_size = m_props->get_i32("Hypertable.Scanner.QueueSize");
  HT_ASSERT(m_scanner_queue_size > 0);


  // Convert table name to ID string

  bool is_namespace;

  if (!m_namemap->name_to_id(m_name, table_id, &is_namespace) ||
      is_namespace)
    HT_THROW(Error::TABLE_NOT_FOUND, m_name);
  m_table.set_id(table_id);

  tablefile = m_toplevel_dir + "/tables/" + m_table.id;

  /**
   * Get schema attribute
   */
  value_buf.clear();
  // TODO: issue 11
  try {
    m_hyperspace->attr_get(tablefile, "schema", value_buf);
  }
  catch (Exception &e) {
    if (e.code() == Error::HYPERSPACE_BAD_PATHNAME)
      HT_THROW2(Error::TABLE_NOT_FOUND, e, m_name);
    HT_THROW2F(e.code(), e, "Unable to open Hyperspace table file '%s'",
               tablefile.c_str());
  }

  m_schema = Schema::new_instance((const char *)value_buf.base,
                                  strlen((const char *)value_buf.base));

  if (!m_schema->is_valid()) {
    HT_ERRORF("Schema Parse Error: %s", m_schema->get_error_string());
    HT_THROW_(Error::SCHEMA_PARSE_ERROR);
  }
  if (m_schema->need_id_assignment())
    HT_THROW(Error::SCHEMA_PARSE_ERROR, "Schema needs ID assignment");

  if (is_index && m_schema->get_version() < 1)
    HT_THROW(Error::BAD_FORMAT, "Unsupported index format.  Indexes must be "
             "rebuilt (see REBUILD INDEX)");

  m_table.generation = m_schema->get_generation();
  m_stale = false;
}

void Table::refresh_if_required() {
  HT_ASSERT(m_name != "");
  if (m_stale)
    initialize();
}

void Table::refresh() {
  ScopedLock lock(m_mutex);
  HT_ASSERT(m_name != "");
  m_stale = true;
  initialize();
}


void Table::get(TableIdentifierManaged &ident_copy, SchemaPtr &schema_copy) {
  ScopedLock lock(m_mutex);
  refresh_if_required();
  ident_copy = m_table;
  schema_copy = m_schema;
}


void
Table::refresh(TableIdentifierManaged &ident_copy, SchemaPtr &schema_copy) {
  ScopedLock lock(m_mutex);
  HT_ASSERT(m_name != "");
  m_stale = true;
  initialize();
  ident_copy = m_table;
  schema_copy = m_schema;
}


Table::~Table() {
}


TableMutator*
Table::create_mutator(uint32_t timeout_ms, uint32_t flags,
                      uint32_t flush_interval_ms) {
  uint32_t timeout = timeout_ms ? timeout_ms : m_timeout_ms;

  HT_ASSERT(needs_index_table() ? has_index_table() : true);
  HT_ASSERT(needs_qualifier_index_table() ? has_qualifier_index_table() : true);

  {
    ScopedLock lock(m_mutex);
    refresh_if_required();
  }

  if (flush_interval_ms) {
    return new TableMutatorShared(m_props, m_comm, this, m_range_locator,
                                  m_app_queue, timeout, flush_interval_ms, flags);
  }
  return new TableMutator(m_props, m_comm, this, m_range_locator, timeout, flags);
}

TableMutatorAsync *
Table::create_mutator_async(ResultCallback *cb, uint32_t timeout_ms, uint32_t flags) {
  uint32_t timeout = timeout_ms ? timeout_ms : m_timeout_ms;

  HT_ASSERT(needs_index_table() ? has_index_table() : true);
  HT_ASSERT(needs_qualifier_index_table() ? has_qualifier_index_table() : true);

  {
    ScopedLock lock(m_mutex);
    refresh_if_required();
  }

  return new TableMutatorAsync(m_props, m_comm, m_app_queue, this, m_range_locator, timeout,
      cb, flags);
}

TableScanner *
Table::create_scanner(const ScanSpec &scan_spec, uint32_t timeout_ms,
                      int32_t flags) {

  {
    ScopedLock lock(m_mutex);
    refresh_if_required();
  }

  return new TableScanner(m_comm, this, m_range_locator, scan_spec,
                          timeout_ms ? timeout_ms : m_timeout_ms);
}

TableScannerAsync *
Table::create_scanner_async(ResultCallback *cb, const ScanSpec &scan_spec, uint32_t timeout_ms,
                            int32_t flags) {
  HT_ASSERT(needs_index_table() ? has_index_table() : true);
  HT_ASSERT(needs_qualifier_index_table() ? has_qualifier_index_table() : true);

  {
    ScopedLock lock(m_mutex);
    refresh_if_required();
  }

  return new TableScannerAsync(m_comm, m_app_queue, this, m_range_locator, scan_spec,
                                timeout_ms ? timeout_ms : m_timeout_ms, cb,
                                flags);
}
