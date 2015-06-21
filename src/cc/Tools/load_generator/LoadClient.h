/*
 * Copyright (C) 2007-2015 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or any later version.
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

#ifndef Tools_load_generator_LoadClient_h
#define Tools_load_generator_LoadClient_h

#include <Common/Compat.h>

#ifdef HT_WITH_THRIFT
#include <ThriftBroker/Client.h>
#include <ThriftBroker/Config.h>
#include <ThriftBroker/ThriftHelper.h>
#endif

#include <Hypertable/Lib/Client.h>
#include <Hypertable/Lib/Config.h>

#include <Common/String.h>

#include <boost/algorithm/string.hpp>

#include <iostream>
#include <fstream>
#include <cstdio>
#include <cmath>
#include <memory>

using namespace Hypertable;
using namespace Hypertable::Config;
using namespace std;
using namespace boost;

class LoadClient {
  public:
    LoadClient(const String &config_file, bool thrift=false);
    LoadClient(bool thrift=false);
    ~LoadClient();

    void create_mutator(const String &tablename, int mutator_flags,
                        ::uint64_t shared_mutator_flush_interval);
    /**
     * Create a scanner.
     * For thrift scanners, just use the 1st column and 1st row_interval specified in the
     * ScanSpec.
     */
    void create_scanner(const String &tablename, const ScanSpec& scan_spec);
    void set_cells(const Cells &cells);
    void set_delete(const KeySpec &key);
    void flush();
    void close_scanner();

    /**
     * Get all cells that match the spec in the current scanner
     * return the total number of bytes scanned
     */
    uint64_t get_all_cells();

  private:
    bool m_thrift;
    ClientPtr m_native_client;
    NamespacePtr m_ns;
    TablePtr m_native_table;
    bool m_native_table_open;
    TableMutatorPtr m_native_mutator;
    TableScannerPtr m_native_scanner;
#ifdef HT_WITH_THRIFT
    boost::shared_ptr<Thrift::Client> m_thrift_client;
    ThriftGen::Namespace m_thrift_namespace;
    ThriftGen::Mutator m_thrift_mutator;
    ThriftGen::Scanner m_thrift_scanner;
#endif
};

typedef std::shared_ptr<LoadClient> LoadClientPtr;

#endif // Tools_load_generator_LoadClient_h
