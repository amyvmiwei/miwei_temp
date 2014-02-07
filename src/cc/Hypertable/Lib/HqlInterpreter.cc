/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License.
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
#include "Schema.h"

#include <cstdio>
#include <cstring>
#include <sstream>
#include <iostream>

extern "C" {
#include <time.h>
}

#include <boost/algorithm/string.hpp>
#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/device/null.hpp>

#include "Common/Config.h"
#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/Stopwatch.h"
#include "Common/ScopeGuard.h"
#include "Common/String.h"

#include "Client.h"
#include "Namespace.h"
#include "HqlInterpreter.h"
#include "HqlHelpText.h"
#include "HqlParser.h"
#include "Key.h"
#include "LoadDataEscape.h"
#include "LoadDataFlags.h"
#include "LoadDataSource.h"
#include "LoadDataSourceFactory.h"
#include "ScanSpec.h"
#include "TableSplit.h"
#include "Types.h"

#include "DfsBroker/Lib/FileDevice.h"

using namespace std;
using namespace Hypertable;
using namespace Hql;

namespace {

void close_file(int fd) {
  if (fd >= 0)
    close(fd);
}

void cmd_help(ParserState &state, HqlInterpreter::Callback &cb) {
  const char **text = HqlHelpText::get(state.str);

  if (text) {
    for (; *text; ++text)
      cb.on_return(*text);
  }
  else
    cb.on_return("\nno help for '" + state.str + "'");
}

void
cmd_create_namespace(Client *client, NamespacePtr &ns, ParserState &state,
    HqlInterpreter::Callback &cb) {

  if (!ns || state.ns.find('/') == 0)
    client->create_namespace(state.ns, NULL, false, state.if_exists);
  else
    client->create_namespace(state.ns, ns.get(), false, state.if_exists);
  cb.on_finish();
}

void
cmd_use_namespace(Client *client, NamespacePtr &ns, bool immutable_namespace,
                  ParserState &state, HqlInterpreter::Callback &cb) {

  if (ns && immutable_namespace)
    HT_THROW(Error::BAD_NAMESPACE, (String)"Attempting to modify immutable namespace " +
             ns->get_name() + " to " + state.ns);

  NamespacePtr tmp_ns;
  if (!ns || state.ns.find('/') == 0)
    tmp_ns = client->open_namespace(state.ns);
  else
    tmp_ns = client->open_namespace(state.ns, ns.get());
  if (tmp_ns)
    ns = tmp_ns;
  else
    HT_THROW(Error::NAMESPACE_DOES_NOT_EXIST, (String)"Couldn't open namespace " + state.ns);

  cb.on_finish();
}

void
cmd_drop_namespace(Client *client, NamespacePtr &ns, ParserState &state,
    HqlInterpreter::Callback &cb) {

  if (!ns || state.ns.find('/') == 0 )
    client->drop_namespace(state.ns, NULL, state.if_exists);
  else
    client->drop_namespace(state.ns, ns.get(), state.if_exists);
  cb.on_finish();
}

void
cmd_rename_table(NamespacePtr &ns, ParserState &state, HqlInterpreter::Callback &cb) {
  if (!ns)
    HT_THROW(Error::BAD_NAMESPACE, "Null namespace");

  ns->rename_table(state.table_name, state.new_table_name);
  cb.on_finish();
}

void
cmd_exists_table(NamespacePtr &ns, ParserState &state, HqlInterpreter::Callback &cb) {
  String exists = (String)"true";

  if (!ns)
    HT_THROW(Error::BAD_NAMESPACE, "Null namespace");

  if (!ns->exists_table(state.table_name))
    exists = (String)"false";
  cb.on_return(exists);
  cb.on_finish();
}

void
cmd_show_create_table(NamespacePtr &ns, ParserState &state,
                      HqlInterpreter::Callback &cb) {
  if (!ns)
    HT_THROW(Error::BAD_NAMESPACE, "Null namespace");
  String out_str;
  String schema_str = ns->get_schema_str(state.table_name, true);
  SchemaPtr schema = Schema::new_instance(schema_str.c_str(),
                                          schema_str.length());
  if (!schema->is_valid())
    HT_THROW(Error::BAD_SCHEMA, schema->get_error_string());

  schema->render_hql_create_table(state.table_name, out_str);
  cb.on_return(out_str);
  cb.on_finish();
}


void
cmd_create_table(NamespacePtr &ns, ParserState &state,
                 HqlInterpreter::Callback &cb) {
  if (!ns)
    HT_THROW(Error::BAD_NAMESPACE, "Null namespace");
  String schema_str;
  SchemaPtr schema;
  bool need_default_ag = false;

  if (!state.clone_table_name.empty()) {
    schema_str = ns->get_schema_str(state.clone_table_name, true);
    schema = Schema::new_instance(schema_str.c_str(), schema_str.size());
    schema_str.clear();
    schema->render(schema_str);
    ns->create_table(state.table_name, schema_str.c_str());
  }
  else {
    schema = new Schema();
    schema->validate_compressor(state.table_compressor);
    schema->set_compressor(state.table_compressor);
    schema->set_group_commit_interval(state.group_commit_interval);

    foreach_ht(Schema::AccessGroup *ag, state.ag_list) {
      schema->validate_compressor(ag->compressor);
      schema->validate_bloom_filter(ag->bloom_filter);
      if (state.table_in_memory)
        ag->in_memory = true;
      if (state.table_blocksize != 0 && ag->blocksize == 0)
        ag->blocksize = state.table_blocksize;
      if (state.table_replication != -1 && ag->replication == -1)
        ag->replication = (int16_t)state.table_replication;
      schema->add_access_group(ag);
    }

    if (state.ag_map.find("default") == state.ag_map.end())
      need_default_ag = true;

    foreach_ht(Schema::ColumnFamily *cf, state.cf_list) {
      if (cf->ag == "") {
        cf->ag = "default";
        if (need_default_ag) {
          Schema::AccessGroup *ag = new Schema::AccessGroup();
          ag->name = "default";
          if (state.table_in_memory)
            ag->in_memory = true;
          if (state.table_blocksize != 0 && ag->blocksize == 0)
            ag->blocksize = state.table_blocksize;
          if (state.table_replication != -1 && ag->replication == -1)
            ag->replication = (int16_t)state.table_replication;
          schema->add_access_group(ag);
          need_default_ag = false;
        }
      }
      if (cf->ttl == 0 && state.ttl != 0)
        cf->ttl = state.ttl;
      if (cf->max_versions == 0 && state.max_versions != 0)
        cf->max_versions = state.max_versions;
      if (cf->counter) {
        if (cf->max_versions)
          HT_THROWF(Error::HQL_PARSE_ERROR,
                    "Incompatible options (COUNTER & MAX_VERSIONS) specified for column '%s'",
                    cf->name.c_str());
        if (cf->time_order_desc_set && cf->time_order_desc)
          HT_THROWF(Error::HQL_PARSE_ERROR,
                    "Incompatible options (COUNTER & TIME_ORDER DESC) specified for column '%s'",
                    cf->name.c_str());
      }
      if (!cf->time_order_desc_set) {
        cf->time_order_desc = state.time_order_desc;
        cf->time_order_desc_set = true;
      }
      schema->add_column_family(cf);
    }

    if (!schema->is_valid())
      HT_THROW(Error::HQL_PARSE_ERROR, schema->get_error_string());

    schema->render(schema_str);
    ns->create_table(state.table_name, schema_str.c_str());
  }
  cb.on_finish();
}

void
cmd_alter_table(NamespacePtr &ns, ParserState &state,
                 HqlInterpreter::Callback &cb) {
  if (!ns)
    HT_THROW(Error::BAD_NAMESPACE, "Null namespace");
  String schema_str;
  SchemaPtr schema = new Schema();
  bool need_default_ag = false;

  foreach_ht(Schema::AccessGroup *ag, state.ag_list)
    schema->add_access_group(ag);

  if (state.ag_map.find("default") == state.ag_map.end())
    need_default_ag = true;

  foreach_ht(Schema::ColumnFamily *cf, state.cf_list) {
    if (cf->ag == "") {
      cf->ag = "default";
      if (need_default_ag) {
        Schema::AccessGroup *ag = new Schema::AccessGroup();
        ag->name = "default";
        schema->add_access_group(ag);
        need_default_ag = false;
      }
    }
    schema->add_column_family(cf);
  }

  const char *error_str = schema->get_error_string();
  if (error_str)
    HT_THROW(Error::HQL_PARSE_ERROR, error_str);

  ns->alter_table(state.table_name, schema);

  /**
   * Refresh the cached table
   */
  ns->refresh_table(state.table_name);
  cb.on_finish();
}

void
cmd_compact(NamespacePtr &ns, ParserState &state,
                 HqlInterpreter::Callback &cb) {
  if (!ns)
    HT_THROW(Error::BAD_NAMESPACE, "Null namespace");
  ns->compact(state.table_name, state.str, state.flags);
  cb.on_finish();
}


void
cmd_describe_table(NamespacePtr &ns, ParserState &state,
                   HqlInterpreter::Callback &cb) {
  if (!ns)
    HT_THROW(Error::BAD_NAMESPACE, "Null namespace");
  String schema_str = ns->get_schema_str(state.table_name, state.with_ids);
  cb.on_return(schema_str);
  cb.on_finish();
}

void
cmd_select(NamespacePtr &ns, ConnectionManagerPtr &conn_manager,
           DfsBroker::ClientPtr &dfs_client, ParserState &state, HqlInterpreter::Callback &cb) {
  if (!ns)
    HT_THROW(Error::BAD_NAMESPACE, "Null namespace");
  TablePtr table;
  TableScannerPtr scanner;
  boost::iostreams::filtering_ostream fout;
  FILE *outf = cb.output;
  int out_fd = -1;
  String dfs = "dfs://";
  String localfs = "file://";
  char fs = state.field_separator ? state.field_separator : '\t';

  table = ns->open_table(state.table_name);
  scanner = table->create_scanner(state.scan.builder.get(), 0, true);

  // whether it's select into file
  if (!state.scan.outfile.empty()) {
    FileUtils::expand_tilde(state.scan.outfile);

    if (boost::algorithm::ends_with(state.scan.outfile, ".gz"))
      fout.push(boost::iostreams::gzip_compressor());

    if (boost::algorithm::starts_with(state.scan.outfile, dfs)) {
      // init Dfs client if not done yet
      if (!dfs_client)
        dfs_client = new DfsBroker::Client(conn_manager, Config::properties);
      fout.push(DfsBroker::FileSink(dfs_client, state.scan.outfile.substr(dfs.size())));
    }
    else if (boost::algorithm::starts_with(state.scan.outfile, localfs))
      fout.push(boost::iostreams::file_descriptor_sink(state.scan.outfile.substr(localfs.size())));
    else
      fout.push(boost::iostreams::file_descriptor_sink(state.scan.outfile));

    if (state.scan.display_timestamps) {
      if (state.scan.keys_only)
        fout << "#timestamp" << fs << "row\n";
      else
        fout << "#timestamp" << fs << "row" << fs << "column" << fs << "value\n";
    }
    else {
      if (state.scan.keys_only)
        fout << "#row\n";
      else
        fout << "#row" << fs << "column" << fs << "value\n";
    }
  }
  else if (!outf) {
    cb.on_scan(*scanner.get());
    return;
  }
  else {
    out_fd = dup(fileno(outf));
    fout.push(boost::iostreams::file_descriptor_sink(out_fd));
  }

  HT_ON_SCOPE_EXIT(&close_file, out_fd);
  Cell cell;
  ::uint32_t nsec;
  time_t unix_time;
  struct tm tms;
  LoadDataEscape row_escaper;
  LoadDataEscape escaper;
  const char *unescaped_buf, *row_unescaped_buf;
  size_t unescaped_len, row_unescaped_len;

  if (fs != '\t') {
    row_escaper.set_field_separator(fs);
    escaper.set_field_separator(fs);
  }

  while (scanner->next(cell)) {
    if (cb.normal_mode) {
      // do some stats
      ++cb.total_cells;
      cb.total_keys_size += strlen(cell.row_key);

      if (cell.column_family && cell.column_qualifier)
        cb.total_keys_size += strlen(cell.column_qualifier) + 1;

      cb.total_values_size += cell.value_len;
    }
    if (state.scan.display_timestamps) {
      if (cb.format_ts_in_nanos) {
        fout << cell.timestamp << fs;
      }
      else {
        nsec = cell.timestamp % 1000000000LL;
        unix_time = cell.timestamp / 1000000000LL;
        localtime_r(&unix_time, &tms);
        fout << Hypertable::format("%d-%02d-%02d %02d:%02d:%02d.%09d%c", 
                        tms.tm_year + 1900, tms.tm_mon + 1, tms.tm_mday, 
                        tms.tm_hour, tms.tm_min, tms.tm_sec, nsec, fs);
      }
    }
    if (state.escape)
      row_escaper.escape(cell.row_key, strlen(cell.row_key),
             &row_unescaped_buf, &row_unescaped_len);
    else
      row_unescaped_buf = cell.row_key;
    if (!state.scan.keys_only) {
      if (cell.column_family && *cell.column_family) {
        fout << row_unescaped_buf << fs << cell.column_family;
        if (cell.column_qualifier && *cell.column_qualifier) {
          if (state.escape)
            escaper.escape(cell.column_qualifier, strlen(cell.column_qualifier),
                &unescaped_buf, &unescaped_len);
          else
            unescaped_buf = cell.column_qualifier;
          fout << ":" << unescaped_buf;
        }

      }
      else
        fout << row_unescaped_buf;

      if (state.escape)
        escaper.escape((const char *)cell.value, (size_t)cell.value_len,
                       &unescaped_buf, &unescaped_len);
      else {
        unescaped_buf = (const char *)cell.value;
        unescaped_len = (size_t)cell.value_len;
      }

      fout << fs;
      switch(cell.flag) {
      case FLAG_INSERT:
        fout.write(unescaped_buf, unescaped_len);
        fout << "\n";
        break;
      case FLAG_DELETE_ROW:
        fout << fs << "DELETE ROW\n";
        break;
       case FLAG_DELETE_COLUMN_FAMILY:
        fout.write(unescaped_buf, unescaped_len);
        fout << fs << "DELETE COLUMN FAMILY\n";
        break;
      case FLAG_DELETE_CELL:
        fout.write(unescaped_buf, unescaped_len);
        fout << fs << "DELETE CELL\n";
        break;
      case FLAG_DELETE_CELL_VERSION:
        fout.write(unescaped_buf, unescaped_len);
        fout << fs << "DELETE CELL VERSION\n";
        break;
      default:
        fout << fs << "BAD KEY FLAG\n";
      }
    }
    else
      fout << row_unescaped_buf << "\n";
  }

  fout.strict_sync();

  cb.on_finish((TableMutator*)0);
}


void
cmd_dump_table(NamespacePtr &ns,
               ConnectionManagerPtr &conn_manager, DfsBroker::ClientPtr &dfs_client,
               ParserState &state, HqlInterpreter::Callback &cb) {
  if (!ns)
    HT_THROW(Error::BAD_NAMESPACE, "Null namespace");
  TablePtr table;
  boost::iostreams::filtering_ostream fout;
  FILE *outf = cb.output;
  int out_fd = -1;
  String dfs = "dfs://";
  String localfs = "file://";
  char fs = state.field_separator ? state.field_separator : '\t';

  // verify parameters

  TableDumperPtr dumper = new TableDumper(ns, state.table_name, state.scan.builder.get());

  // whether it's select into file
  if (!state.scan.outfile.empty()) {
    FileUtils::expand_tilde(state.scan.outfile);

    if (boost::algorithm::ends_with(state.scan.outfile, ".gz"))
      fout.push(boost::iostreams::gzip_compressor());
    if (boost::algorithm::starts_with(state.scan.outfile, dfs)) {
      // init Dfs client if not done yet
      if (!dfs_client)
        dfs_client = new DfsBroker::Client(conn_manager, Config::properties);
      fout.push(DfsBroker::FileSink(dfs_client, state.scan.outfile.substr(dfs.size())));
    }
    else if (boost::algorithm::starts_with(state.scan.outfile, localfs))
      fout.push(boost::iostreams::file_descriptor_sink(state.scan.outfile.substr(localfs.size())));
    else
      fout.push(boost::iostreams::file_descriptor_sink(state.scan.outfile));

    if (state.scan.display_timestamps)
      fout << "#timestamp" << fs << "row" << fs << "column" << fs << "value\n";
    else
      fout << "#row" << fs << "column" << fs << "value\n";
  }
  else if (!outf) {
    cb.on_dump(*dumper.get());
    return;
  }
  else {
    out_fd = dup(fileno(outf));
    fout.push(boost::iostreams::file_descriptor_sink(out_fd));
  }

  HT_ON_SCOPE_EXIT(&close_file, out_fd);
  Cell cell;
  LoadDataEscape row_escaper;
  LoadDataEscape escaper;
  const char *unescaped_buf, *row_unescaped_buf;
  size_t unescaped_len, row_unescaped_len;

  if (fs != '\t') {
    row_escaper.set_field_separator(fs);
    escaper.set_field_separator(fs);
  }

  while (dumper->next(cell)) {
    if (cb.normal_mode) {
      // do some stats
      ++cb.total_cells;
      cb.total_keys_size += strlen(cell.row_key);

      if (cell.column_family && cell.column_qualifier)
        cb.total_keys_size += strlen(cell.column_qualifier) + 1;

      cb.total_values_size += cell.value_len;
    }

    if (state.scan.display_timestamps)
      fout << cell.timestamp << fs;

    if (state.escape)
      row_escaper.escape(cell.row_key, strlen(cell.row_key),
             &row_unescaped_buf, &row_unescaped_len);
    else
      row_unescaped_buf = cell.row_key;

    if (cell.column_family) {
      fout << row_unescaped_buf << fs << cell.column_family;
      if (cell.column_qualifier && *cell.column_qualifier) {
        if (state.escape)
          escaper.escape(cell.column_qualifier, strlen(cell.column_qualifier),
                   &unescaped_buf, &unescaped_len);
        else
          unescaped_buf = cell.column_qualifier;
        fout << ":" << unescaped_buf;
      }
    }
    else
      fout << row_unescaped_buf;

    if (state.escape)
      escaper.escape((const char *)cell.value, (size_t)cell.value_len,
             &unescaped_buf, &unescaped_len);
    else {
      unescaped_buf = (const char *)cell.value;
      unescaped_len = (size_t)cell.value_len;
    }

    HT_ASSERT(cell.flag == FLAG_INSERT);

    fout << fs ;
    fout.write(unescaped_buf, unescaped_len);
    fout << "\n";
  }

  fout.strict_sync();

  cb.on_finish((TableMutator*)0);
}

void
cmd_load_data(NamespacePtr &ns, ::uint32_t mutator_flags,
              ConnectionManagerPtr &conn_manager, 
              DfsBroker::ClientPtr &dfs_client,
              ParserState &state, HqlInterpreter::Callback &cb) {
  if (!ns)
    HT_THROW(Error::BAD_NAMESPACE, "Null namespace");
  TablePtr table;
  TableMutatorPtr mutator;
  bool into_table = true;
  bool display_timestamps = false;
  boost::iostreams::filtering_ostream fout;
  FILE *outf = cb.output;
  int out_fd = -1;
  bool largefile_mode = false;
  ::uint64_t running_total = 0;
  ::uint64_t consume_threshold = 0;
  bool ignore_unknown_columns = false;
  char fs = state.field_separator ? state.field_separator : '\t';

  if (LoadDataFlags::ignore_unknown_cfs(state.load_flags))
    ignore_unknown_columns = true;

  // Turn on no-log-sync unconditionally for LOAD DATA INFILE
  mutator_flags |= Table::MUTATOR_FLAG_NO_LOG_SYNC;

  if (state.table_name.empty()) {
    if (state.output_file.empty())
      HT_THROW(Error::HQL_PARSE_ERROR,
          "LOAD DATA INFILE ... INTO FILE - bad filename");
    fout.push(boost::iostreams::file_descriptor_sink(state.output_file));
    into_table = false;
  }
  else {
    if (outf) {
      out_fd = dup(fileno(outf));
      fout.push(boost::iostreams::file_descriptor_sink(out_fd));
    }
    else
      fout.push(boost::iostreams::null_sink());
    table = ns->open_table(state.table_name);
    mutator = table->create_mutator(0, mutator_flags);
  }

  HT_ON_SCOPE_EXIT(&close_file, out_fd);


  LoadDataSourcePtr lds;
  bool is_delete;

  // init Dfs client if not done yet
  if(state.input_file_src == DFS_FILE && !dfs_client)
    dfs_client = new DfsBroker::Client(conn_manager, Config::properties);

  lds = LoadDataSourceFactory::create(dfs_client, state.input_file,
               state.input_file_src, state.header_file, state.header_file_src,
               state.columns, state.timestamp_column, fs,
               state.row_uniquify_chars, state.load_flags);

  cb.file_size = lds->get_source_size();
  if (cb.file_size > std::numeric_limits<unsigned long>::max()) {
    largefile_mode = true;
    unsigned long adjusted_size = (unsigned long)(cb.file_size / 1048576LL);
    consume_threshold = 1048576LL;
    cb.on_update(adjusted_size);
  }
  else
    cb.on_update(cb.file_size);

  if (!into_table) {
    display_timestamps = lds->has_timestamps();
    if (display_timestamps)
      fout << "timestamp" << fs << "row" << fs << "column" << fs << "value\n";
    else
      fout << "row" << fs << "column" << fs << "value\n";
  }

  KeySpec key;
  ::uint8_t *value;
  ::uint32_t value_len;
  ::uint32_t consumed = 0;
  LoadDataEscape row_escaper;
  LoadDataEscape qualifier_escaper;
  LoadDataEscape value_escaper;
  const char *escaped_buf;
  size_t escaped_len;

  if (fs != '\t') {
    row_escaper.set_field_separator(fs);
    qualifier_escaper.set_field_separator(fs);
    value_escaper.set_field_separator(fs);
  }

  try {

    while (lds->next(&key, &value, &value_len, &is_delete, &consumed)) {

      ++cb.total_cells;
      cb.total_values_size += value_len;
      cb.total_keys_size += key.row_len;

      if (state.escape) {
        row_escaper.unescape((const char *)key.row, (size_t)key.row_len,
            &escaped_buf, &escaped_len);
        key.row = escaped_buf;
        key.row_len = escaped_len;
        qualifier_escaper.unescape(key.column_qualifier, 
                (size_t)key.column_qualifier_len, &escaped_buf, &escaped_len);
        key.column_qualifier = escaped_buf;
        key.column_qualifier_len = escaped_len;
        value_escaper.unescape((const char *)value, 
                (size_t)value_len, &escaped_buf, &escaped_len);
      }
      else {
        escaped_buf = (const char *)value;
        escaped_len = (size_t)value_len;
      }

      if (into_table) {
        try {
          bool skip = false;
          if (ignore_unknown_columns) {
            SchemaPtr schema = table->schema();
            if (!schema->get_column_family(key.column_family))
              skip = true;
          }
          if (!skip) {
            if (is_delete)
              mutator->set_delete(key);
            else
              mutator->set(key, escaped_buf, escaped_len);
          }
        }
        catch (Exception &e) {
          do {
            mutator->show_failed(e);
          } while (!mutator->retry());
        }
      }
      else {
        if (display_timestamps)
          fout << key.timestamp << fs << key.row << fs
               << key.column_family << fs << escaped_buf << "\n";
        else
          fout << key.row << fs << key.column_family << fs
               << escaped_buf << "\n";
      }

      if (cb.normal_mode && state.input_file_src != STDIN) {
        if (largefile_mode == true) {
          running_total += consumed;
          if (running_total >= consume_threshold) {
            consumed = 1 + (unsigned long)((running_total - consume_threshold) 
                    / 1048576LL);
            consume_threshold += (::uint64_t)consumed * 1048576LL;
            cb.on_progress(consumed);
          }
        }
        else
          cb.on_progress(consumed);
      }
    }
  }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "line number %lld", (Lld)lds->get_current_lineno());
  }

  fout.strict_sync();

  cb.on_finish(mutator.get());
}

void
cmd_insert(NamespacePtr &ns, ParserState &state, HqlInterpreter::Callback &cb) {
  if (!ns)
    HT_THROW(Error::BAD_NAMESPACE, "Null namespace");
  TablePtr table;
  TableMutatorPtr mutator;
  const Cells &cells = state.inserts.get();

  table = ns->open_table(state.table_name);
  mutator = table->create_mutator();

  try {
    mutator->set_cells(cells);
  }
  catch (Exception &e) {
    do {
      mutator->show_failed(e);
    } while (!mutator->retry());

    if (mutator->get_last_error())
      HT_THROW(mutator->get_last_error(),
              Error::get_text(mutator->get_last_error()));
  }
  if (cb.normal_mode) {
    cb.total_cells = cells.size();

    foreach_ht(const Cell &cell, cells) {
      cb.total_keys_size += cell.column_qualifier
          ? (strlen(cell.column_qualifier) + 1) : 0;
      cb.total_values_size += cell.value_len;
    }
  }

  // flush the mutator
  cb.on_finish(mutator.get());

  // report errors during mutator->flush
  if (mutator->get_last_error())
    HT_THROW(mutator->get_last_error(),
            Error::get_text(mutator->get_last_error()));
}

void
cmd_delete(NamespacePtr &ns, ParserState &state, HqlInterpreter::Callback &cb) {
  if (!ns)
    HT_THROW(Error::BAD_NAMESPACE, "Null namespace");
  TablePtr table;
  TableMutatorPtr mutator;
  KeySpec key;
  char *column_qualifier;

  table = ns->open_table(state.table_name);
  mutator = table->create_mutator();

  key.row = state.delete_row.c_str();
  key.row_len = state.delete_row.length();

  if (state.delete_version_time) {
    key.flag = FLAG_DELETE_CELL_VERSION;
    key.timestamp = state.delete_version_time;
  }
  else if (state.delete_time) {
    key.flag = FLAG_DELETE_CELL;
    key.timestamp = state.delete_time;
  }
  else
    key.timestamp = AUTO_ASSIGN;

  if (state.delete_all_columns) {
    try {
      key.flag = FLAG_DELETE_ROW;
      mutator->set_delete(key);
    }
    catch (Exception &e) {
      mutator->show_failed(e);
      return;
    }
  }
  else {
    foreach_ht(const String &col, state.delete_columns) {
      ++cb.total_cells;

      key.column_family = col.c_str();
      if ((column_qualifier = (char*)strchr(col.c_str(), ':')) != 0) {
        *column_qualifier++ = 0;
        key.column_qualifier = column_qualifier;
        key.column_qualifier_len = strlen(column_qualifier);
        if (key.flag == FLAG_INSERT)
          key.flag = FLAG_DELETE_CELL;
      }
      else {
        key.column_qualifier = 0;
        key.column_qualifier_len = 0;
        if (key.flag == FLAG_INSERT)
          key.flag = FLAG_DELETE_COLUMN_FAMILY;
      }
      try {
        mutator->set_delete(key);
      }
      catch (Exception &e) {
        mutator->show_failed(e);
        return;
      }
    }
  }

  cb.on_finish(mutator.get());
}

void
cmd_get_listing(NamespacePtr &ns, ParserState &state,
    HqlInterpreter::Callback &cb) {
  if (!ns)
    HT_THROW(Error::BAD_NAMESPACE, "Null namespace");
  std::vector<NamespaceListing> listing;
  ns->get_listing(false, listing);
  foreach_ht (const NamespaceListing &entry, listing) {
    if (entry.is_namespace && !state.tables_only)
      cb.on_return(entry.name + "\t(namespace)");
    else if (!entry.is_namespace)
      cb.on_return(entry.name);
  }
  cb.on_finish();
}

void
cmd_drop_table(NamespacePtr &ns, ParserState &state,
               HqlInterpreter::Callback &cb) {
  if (!ns)
    HT_THROW(Error::BAD_NAMESPACE, "Null namespace");

  ns->drop_table(state.table_name, state.if_exists);
  cb.on_finish();
}

void
cmd_balance(Client *client, ParserState &state,
            HqlInterpreter::Callback &cb) {
  MasterClientPtr master = client->get_master_client();

  master->balance(state.balance_plan);

  cb.on_finish();
}

void
cmd_stop(Client *client, ParserState &state, HqlInterpreter::Callback &cb) {
  MasterClientPtr master = client->get_master_client();

  master->stop(state.rs_name, false);

  cb.on_finish();
}

void
cmd_set(Client *client, ParserState &state, HqlInterpreter::Callback &cb) {
  MasterClientPtr master = client->get_master_client();

  master->set(state.variable_specs);

  cb.on_finish();
}

void cmd_shutdown_master(Client *client, HqlInterpreter::Callback &cb) {
  client->shutdown();
  cb.on_finish();
}

void cmd_close(Client *client, HqlInterpreter::Callback &cb) {
  client->close();
  cb.on_finish();
}

} // local namespace


HqlInterpreter::HqlInterpreter(Client *client, ConnectionManagerPtr &conn_manager,
    bool immutable_namespace) : m_client(client), m_mutator_flags(0),
    m_conn_manager(conn_manager), m_dfs_client(0), m_immutable_namespace(immutable_namespace) {
  if (Config::properties->get_bool("Hypertable.HqlInterpreter.Mutator.NoLogSync"))
    m_mutator_flags = Table::MUTATOR_FLAG_NO_LOG_SYNC;

}

void HqlInterpreter::set_namespace(const String &ns) {
  if (m_namespace && m_immutable_namespace)
    HT_THROW(Error::BAD_NAMESPACE, (String)"Attempting to modify immutable namespace " +
             m_namespace->get_name() + " to " + ns);
  m_namespace = m_client->open_namespace(ns);
}

void HqlInterpreter::execute(const String &line, Callback &cb) {
  ParserState state;
  String stripped_line = line;

  boost::trim(stripped_line);

  Hql::Parser parser(state);
  parse_info<> info = parse(stripped_line.c_str(), parser, space_p);

  if (info.full) {
    cb.on_parsed(state);

    switch (state.command) {
    case COMMAND_SHOW_CREATE_TABLE:
      cmd_show_create_table(m_namespace, state, cb);               break;
    case COMMAND_HELP:
      cmd_help(state, cb);                                         break;
    case COMMAND_EXISTS_TABLE:
      cmd_exists_table(m_namespace, state, cb);                    break;
    case COMMAND_CREATE_TABLE:
      cmd_create_table(m_namespace, state, cb);                    break;
    case COMMAND_DESCRIBE_TABLE:
      cmd_describe_table(m_namespace, state, cb);                  break;
    case COMMAND_SELECT:
      cmd_select(m_namespace, m_conn_manager, m_dfs_client,
                 state, cb);                                       break;
    case COMMAND_LOAD_DATA:
      cmd_load_data(m_namespace, m_mutator_flags,
                    m_conn_manager, m_dfs_client, state, cb);      break;
    case COMMAND_INSERT:
      cmd_insert(m_namespace, state, cb);                          break;
    case COMMAND_DELETE:
      cmd_delete(m_namespace, state, cb);                          break;
    case COMMAND_GET_LISTING:
      cmd_get_listing(m_namespace, state, cb);                     break;
    case COMMAND_ALTER_TABLE:
      cmd_alter_table(m_namespace, state, cb);                     break;
    case COMMAND_COMPACT:
      cmd_compact(m_namespace, state, cb);                         break;
    case COMMAND_DROP_TABLE:
      cmd_drop_table(m_namespace, state, cb);                      break;
    case COMMAND_RENAME_TABLE:
      cmd_rename_table(m_namespace, state, cb);                    break;
    case COMMAND_DUMP_TABLE:
      cmd_dump_table(m_namespace, m_conn_manager, m_dfs_client,
                     state, cb);                                   break;
    case COMMAND_CLOSE:
      cmd_close(m_client, cb);                                     break;
    case COMMAND_SHUTDOWN_MASTER:
      cmd_shutdown_master(m_client, cb);                           break;
    case COMMAND_CREATE_NAMESPACE:
      cmd_create_namespace(m_client, m_namespace, state, cb);      break;
    case COMMAND_USE_NAMESPACE:
      cmd_use_namespace(m_client, m_namespace,
                        m_immutable_namespace, state, cb);         break;
    case COMMAND_DROP_NAMESPACE:
      cmd_drop_namespace(m_client, m_namespace, state, cb);        break;
    case COMMAND_BALANCE:
      cmd_balance(m_client, state, cb);                            break;
    case COMMAND_STOP:
      cmd_stop(m_client, state, cb);                               break;
    case COMMAND_SET:
      cmd_set(m_client, state, cb);                                break;

    default:
      HT_THROW(Error::HQL_PARSE_ERROR, String("unsupported command: ") + stripped_line);
    }
  }
  else
    HT_THROW(Error::HQL_PARSE_ERROR, String("parse error at: ") + info.stop + " (" + stripped_line + ")");
}
