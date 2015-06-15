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

#ifndef Hypertable_Lib_HqlInterpreter_h
#define Hypertable_Lib_HqlInterpreter_h

#include <Hypertable/Lib/Cells.h>
#include <Hypertable/Lib/TableDumper.h>
#include <Hypertable/Lib/TableScanner.h>
#include <Hypertable/Lib/TableMutator.h>

#include <FsBroker/Lib/Client.h>

#include <AsyncComm/ConnectionManager.h>

#include <memory>
#include <vector>

namespace Hypertable {

  class Client;
  using namespace std;

  namespace Hql { class ParserState; }

  /**
   * The API of HQL interpreter
   */
  class HqlInterpreter {
  public:
    /** Callback interface/base class for execute */
    struct Callback {
      FILE *output;             // default is NULL
      bool normal_mode;         // default true
      bool format_ts_in_nanos;  // default false
      // mutator stats
      uint64_t total_cells,
               total_keys_size,
               total_values_size,
               file_size;

      Callback(bool normal = true) : output(0), normal_mode(normal),
          format_ts_in_nanos(false), total_cells(0), total_keys_size(0),
          total_values_size(0), file_size(0) { }
      virtual ~Callback() { }

      /** Called when the hql string is parsed successfully */
      virtual void on_parsed(Hql::ParserState &) { }

      /** Called when interpreter returns a string result
       * Maybe called multiple times for a list of string results
       */
      virtual void on_return(const std::string &) { }

      /** Called when interpreter is ready to scan */
      virtual void on_scan(TableScannerPtr &) { }

      /** Called when interpreter is ready to dump */
      virtual void on_dump(TableDumper &) { }

      /** Called when interpreter is ready to update */
      virtual void on_update(size_t total) { }

      /** Called when interpreter updates progress for long running queries */
      virtual void on_progress(size_t amount) { }

      /** Called when interpreter is finished
       * Note: mutator pointer maybe NULL in case of things like
       * LOAD DATA ... INTO file
       */
      virtual void on_finish(TableMutatorPtr &mutator) {
        if (mutator) {
          try {
            mutator->flush();
          }
          catch (Exception &e) {
            mutator->show_failed(e);
            throw;
          }
        }
      }

      virtual void on_finish() { }

      /** Called when scan is finished. */
      virtual void on_finish(TableScannerPtr &scanner) {  }

    };

    /** An example for simple queries that returns small number of results */
    struct SmallCallback : Callback {
      CellsBuilder &cells;
      std::vector<String> &retstrs;

      SmallCallback(CellsBuilder &builder, std::vector<String> &strs)
        : cells(builder), retstrs(strs) { }

      void on_return(const std::string &ret) override { retstrs.push_back(ret); }
      void on_scan(TableScannerPtr &scanner) override { copy(*scanner, cells); }
      void on_dump(TableDumper &dumper) override { copy(dumper, cells); }
    };

    /** Construct from hypertable client */
    HqlInterpreter(Client *client, ConnectionManagerPtr &conn_mgr,
                   bool immutable_namespace=true);

    /** The main interface for the interpreter */
    int execute(const std::string &str, Callback &);

    /** A convenient method demonstrate the usage of the interface */
    int execute(const std::string &str, CellsBuilder &output, std::vector<String> &ret) {
      SmallCallback cb(output, ret);
      return execute(str, cb);
    }

    /** More convenient method for admin commands (create/drop table etc.) */
    int execute(const std::string &cmd) {
      CellsBuilder cb;
      std::vector<String> res;
      return execute(cmd, cb, res);
    }

    void set_namespace(const std::string &ns);

  private:
    Client *m_client;
    NamespacePtr m_namespace;
    uint32_t m_mutator_flags;
    ConnectionManagerPtr m_conn_manager;
    FsBroker::Lib::ClientPtr m_fs_client;
    bool m_immutable_namespace;
  };

  /// Smart pointer to HqlInterpreter
  typedef std::shared_ptr<HqlInterpreter> HqlInterpreterPtr;

}

#endif // Hypertable_Lib_HqlInterpreter_h
