/*
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

/// @file
/// Definitions for general-purpose utility functions.
/// This file contains definitions for a set of general-purpose utility
/// functions used by the %Master.

#include <Common/Compat.h>
#include "Utility.h"

#include <Hypertable/Master/Context.h>

#include <Hypertable/Lib/Key.h>
#include <Hypertable/Lib/KeySpec.h>
#include <Hypertable/Lib/QualifiedRangeSpec.h>
#include <Hypertable/Lib/RangeServer/Client.h>
#include <Hypertable/Lib/RangeSpec.h>
#include <Hypertable/Lib/Schema.h>
#include <Hypertable/Lib/TableIdentifier.h>
#include <Hypertable/Lib/TableMutator.h>
#include <Hypertable/Lib/TableScanner.h>

#include <Hyperspace/Session.h>

#include <AsyncComm/CommAddress.h>

#include <Common/FailureInducer.h>
#include <Common/ScopeGuard.h>
#include <Common/StatusPersister.h>
#include <Common/StringExt.h>
#include <Common/md5.h>
#include <Common/Time.h>

using namespace Hyperspace;

namespace Hypertable { namespace Utility {

void get_table_server_set(ContextPtr &context, const String &id,
                          const String &row, StringSet &servers) {
  String start_row, end_row;
  ScanSpec scan_spec;
  RowInterval ri;
  TableScannerPtr scanner;
  Cell cell;
  String location;

  if (context->test_mode) {
    context->get_available_servers(servers);
    return;
  }

  if (row.empty()) {
    start_row = format("%s:", id.c_str());
    scan_spec.row_limit = 0;
  }
  else {
    start_row = format("%s:%s", id.c_str(), row.c_str());
    scan_spec.row_limit = 1;
  }

  end_row = format("%s:%s", id.c_str(), Key::END_ROW_MARKER);

  scan_spec.max_versions = 1;
  scan_spec.columns.clear();
  scan_spec.columns.push_back("Location");

  ri.start = start_row.c_str();
  ri.end = end_row.c_str();
  scan_spec.row_intervals.push_back(ri);

  scanner.reset( context->metadata_table->create_scanner(scan_spec) );

  while (scanner->next(cell)) {
    location = String((const char *)cell.value, cell.value_len);
    boost::trim(location);
    if (location != "!")
      servers.insert(location);
  }
}

bool table_exists(ContextPtr &context, const String &name, String &id) {
  bool is_namespace;

  id = "";

  if (!context->namemap->name_to_id(name, id, &is_namespace) ||
      is_namespace)
    return false;

  String tablefile = context->toplevel_dir + "/tables/" + id;

  try {
    if (context->hyperspace->attr_exists(tablefile, "x"))
      return true;
  }
  catch (Exception &e) {
    if (e.code() == Error::HYPERSPACE_FILE_NOT_FOUND ||
        e.code() == Error::HYPERSPACE_BAD_PATHNAME)
      return false;
    HT_THROW2(e.code(), e, name);
  }
  return false;
}

TableParts get_index_parts(const std::string &schema) {
  uint8_t parts {};
  SchemaPtr s(Schema::new_instance(schema));

  for (auto cf_spec : s->get_column_families()) {
    if (cf_spec && !cf_spec->get_deleted()) {
      if (cf_spec->get_value_index())
        parts |= TableParts::VALUE_INDEX;
      if (cf_spec->get_qualifier_index())
        parts |= TableParts::QUALIFIER_INDEX;
    }
  }
  return TableParts(parts);
}


bool table_exists(ContextPtr &context, const String &id) {

  String tablefile = context->toplevel_dir + "/tables/" + id;

  try {
    if (context->hyperspace->attr_exists(tablefile, "x"))
      return true;
  }
  catch (Exception &e) {
    if (e.code() == Error::HYPERSPACE_FILE_NOT_FOUND ||
        e.code() == Error::HYPERSPACE_BAD_PATHNAME)
      return false;
    HT_THROW2(e.code(), e, id);
  }
  return false;
}


void verify_table_name_availability(ContextPtr &context, const String &name, String &id) {
  bool is_namespace;

  id = "";

  if (!context->namemap->name_to_id(name, id, &is_namespace))
    return;

  if (is_namespace)
    HT_THROW(Error::NAME_ALREADY_IN_USE, "");

  String tablefile = context->toplevel_dir + "/tables/" + id;

  try {
    if (context->hyperspace->attr_exists(tablefile, "x"))
      HT_THROW(Error::MASTER_TABLE_EXISTS, name);
  }
  catch (Exception &e) {
    if (e.code() != Error::HYPERSPACE_FILE_NOT_FOUND)
      HT_THROW2(e.code(), e, id);
  }
}


void create_table_in_hyperspace(ContextPtr &context, const String &name,
                                const String &schema_str, TableIdentifierManaged *table) {
  String table_name = name;

  // Strip leading '/'
  if (table_name[0] == '/')
    table_name = table_name.substr(1);

  String table_id;
  Utility::verify_table_name_availability(context, table_name, table_id);

  if (table_id == "")
    context->namemap->add_mapping(table_name, table_id, 0, true);

  HT_MAYBE_FAIL("Utility-create-table-in-hyperspace-1");



  HT_ASSERT(table_name != TableIdentifier::METADATA_NAME ||
            table_id == TableIdentifier::METADATA_ID);

  // Create table file
  String tablefile = context->toplevel_dir + "/tables/" + table_id;
  int oflags = OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_CREATE;

  // Write schema attribute
  context->hyperspace->attr_set(tablefile, oflags, "schema", schema_str.c_str(),
                                schema_str.length());

  HT_MAYBE_FAIL("Utility-create-table-in-hyperspace-2");

  // Create /hypertable/tables/<table>/<accessGroup> directories
  // for this table in DFS
  String table_basedir = context->toplevel_dir + "/tables/" + table_id + "/";

  SchemaPtr schema(Schema::new_instance(schema_str));

  for (auto ag_spec : schema->get_access_groups()) {
    String agdir = table_basedir + ag_spec->get_name();
    context->dfs->mkdirs(agdir);
  }

  table->set_id(table_id);
  table->generation = schema->get_generation();

}

void prepare_index(ContextPtr &context, const String &name,
                   const String &schema_str, bool qualifier,
                   String &index_name, String &index_schema_str)
{
  // load the schema of the primary table
  SchemaPtr primary_schema( Schema::new_instance(schema_str) );

  // create a new schema and fill it 
  AccessGroupSpec *ag_spec = new AccessGroupSpec("default");
  ColumnFamilySpec *new_cf_spec = new ColumnFamilySpec("v1");
  new_cf_spec->set_access_group("default");
  ag_spec->add_column(new_cf_spec);

  Schema *index_schema = new Schema();

  // Merge defaults
  index_schema->access_group_defaults().merge( primary_schema->access_group_defaults() );
  index_schema->column_family_defaults().merge( primary_schema->column_family_defaults() );
    
  index_schema->add_access_group(ag_spec);
  index_schema->set_group_commit_interval(
          primary_schema->get_group_commit_interval());
  index_schema->set_version(1);
  int64_t generation = get_ts64();
  index_schema->update_generation(generation);

  // the index table name is prepended with ^, and the qualified
  // index with ^^
  index_name = Filesystem::dirname(name);
  if (qualifier) {
    if (index_name == "/")
      index_name += String("^^") + Filesystem::basename(name);
    else
      index_name += String("/^^") + Filesystem::basename(name);
  }
  else {
    if (index_name == "/")
      index_name += String("^") + Filesystem::basename(name);
    else
      index_name += String("/^") + Filesystem::basename(name);
  }

  index_schema_str = index_schema->render_xml();
  delete index_schema;
}

void create_table_write_metadata(ContextPtr &context, TableIdentifier *table) {

  if (context->test_mode) {
    HT_WARN("Skipping create_table_write_metadata due to TEST MODE");
    return;
  }

  TableMutatorPtr mutator_ptr(context->metadata_table->create_mutator());

  String metadata_key_str = String(table->id) + ":" + Key::END_ROW_MARKER;
  String start_row;
  KeySpec key;
  key.row = metadata_key_str.c_str();
  key.row_len = metadata_key_str.length();
  key.column_qualifier = 0;
  key.column_qualifier_len = 0;
  key.column_family = "StartRow";

  if (table->is_metadata())
    mutator_ptr->set(key, Key::END_ROOT_ROW, strlen(Key::END_ROOT_ROW));
  else
    mutator_ptr->set(key, 0, 0);

  mutator_ptr->flush();
}

/**
 */

bool next_available_server(ContextPtr &context, String &location, bool urgent) {
  RangeServerConnectionPtr rsc;
  if (!context->rsc_manager->next_available_server(rsc, urgent))
    return false;
  location = rsc->location();
  return true;
}


void create_table_load_range(ContextPtr &context, const String &location,
                             TableIdentifier &table, RangeSpec &range, bool needs_compaction) {
  Lib::RangeServer::Client rsc(context->comm);
  CommAddress addr;

  if (context->test_mode) {
    RangeServerConnectionPtr rsc;
    HT_WARNF("Skipping %s::load_range() because in TEST MODE", location.c_str());
    HT_ASSERT(context->rsc_manager->find_server_by_location(location, rsc));
    if (!rsc->connected())
      HT_THROW(Error::COMM_NOT_CONNECTED, "");
    return;
  }

  addr.set_proxy(location);

  try {
    RangeState range_state;
    int64_t split_size = context->props->get_i64("Hypertable.RangeServer.Range.SplitSize");

    if (table.is_metadata())
      range_state.soft_limit = context->props->get_i64("Hypertable.RangeServer.Range.MetadataSplitSize", split_size);
    else {
      range_state.soft_limit = split_size;
      if (context->props->get_bool("Hypertable.Master.Split.SoftLimitEnabled"))
        range_state.soft_limit /= std::min(64, (int)context->rsc_manager->server_count()*2);
    }
      
    rsc.load_range(addr, table, range, range_state, needs_compaction);
  }
  catch (Exception &e) {
    if (e.code() != Error::RANGESERVER_RANGE_ALREADY_LOADED)
      HT_THROW2F(e.code(), e, "Problem loading range %s[%s..%s] in %s",
                 table.id, range.start_row, range.end_row, location.c_str());
  }
}

void create_table_acknowledge_range(ContextPtr &context, const String &location,
                                    TableIdentifier &table, RangeSpec &range) {
  Lib::RangeServer::Client rsc(context->comm);
  CommAddress addr;

  if (context->test_mode) {
    RangeServerConnectionPtr rsc;
    HT_WARNF("Skipping %s::acknowledge_load() because in TEST MODE", location.c_str());
    HT_ASSERT(context->rsc_manager->find_server_by_location(location, rsc));
    if (!rsc->connected())
      HT_THROW(Error::COMM_NOT_CONNECTED, "");
    return;
  }

  addr.set_proxy(location);

  QualifiedRangeSpec qrs(table, range);
  vector<QualifiedRangeSpec *> range_vec;
  map<QualifiedRangeSpec, int> response_map;
  range_vec.push_back(&qrs);
  rsc.acknowledge_load(addr, range_vec, response_map);
  map<QualifiedRangeSpec, int>::iterator it = response_map.begin();
  if (it->second != Error::OK &&
      it->second != Error::TABLE_NOT_FOUND &&
      it->second != Error::RANGESERVER_RANGE_NOT_FOUND)
    HT_THROW(it->second, "Problem acknowledging load range");
}


int64_t range_hash_code(const TableIdentifier &table, const RangeSpec &range, const String &qualifier) {
  if (!qualifier.empty())
    return md5_hash(qualifier.c_str()) ^ md5_hash(table.id) ^ md5_hash(range.start_row) ^ md5_hash(range.end_row);
  return md5_hash(table.id) ^ md5_hash(range.start_row) ^ md5_hash(range.end_row);
}

String range_hash_string(const TableIdentifier &table, const RangeSpec &range, const String &qualifier) {
  return String("") + range_hash_code(table, range, qualifier);
}

String root_range_location(ContextPtr &context) {
  DynamicBuffer value(0);
  String location;
  uint64_t root_handle=0;
  String toplevel_dir = context->props->get_str("Hypertable.Directory");

  try {
    HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, context->hyperspace, &root_handle);
    root_handle = context->hyperspace->open(toplevel_dir + "/root", OPEN_FLAG_READ);
    context->hyperspace->attr_get(root_handle, "Location", value);
    location = (const char *)value.base;
  }
  catch (Exception &e) {
    HT_ERROR_OUT << "Unable to read root location -" << e.what() << HT_END;
    HT_THROW(e.code(), e.what());
  }
  return location;
}

bool status(ContextPtr &context, Timer &timer, Status &status) {
  if (context->startup_in_progress())
    status.set(Status::Code::CRITICAL, Status::Text::SERVER_IS_COMING_UP);
  else if (context->shutdown_in_progress())
    status.set(Status::Code::CRITICAL, Status::Text::SERVER_IS_SHUTTING_DOWN);
  else if (!context->master_file->lock_acquired())
    status.set(Status::Code::OK, Status::Text::STANDBY);
  else if (context->quorum_reached) {
    size_t connected_servers = context->available_server_count();
    size_t total_servers = context->rsc_manager->server_count();
    if (connected_servers < total_servers) {
      size_t failover_pct
        = context->props->get_i32("Hypertable.Failover.Quorum.Percentage");
      size_t quorum = ((total_servers * failover_pct) + 99) / 100;
      if (connected_servers == 0)
        status.set(Status::Code::CRITICAL,
                   "RangeServer recovery blocked because 0 servers available.");
      else if (connected_servers < quorum)
        status.set(Status::Code::CRITICAL,
                   format("RangeServer recovery blocked (%d servers "
                          "available, quorum of %d is required)",
                          (int)connected_servers, (int)quorum));
      else
        status.set(Status::Code::WARNING, "RangeServer recovery in progress");
    }
    else {
      context->dfs->status(status, &timer);
      Status::Code code;
      string text;
      status.get(&code, text);
      if (code != Status::Code::OK)
        status.set(code, format("FsBroker %s", text.c_str()));
      else
        StatusPersister::get(status);
    }
  }
  return status.get() == Status::Code::OK;
}

void shutdown_rangeserver(ContextPtr &context, CommAddress &addr) {
  try {
    Lib::RangeServer::Client rsc(context->comm);
    rsc.shutdown(addr);
  }
  catch (Exception &e) {
  }
}

}}
