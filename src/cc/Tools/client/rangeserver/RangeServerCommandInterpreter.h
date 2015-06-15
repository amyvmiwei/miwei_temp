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

#ifndef Tools_client_rangeserver_RangeServerCommandInterpreter_h
#define Tools_client_rangeserver_RangeServerCommandInterpreter_h

#include "TableInfo.h"

#include <Tools/Lib/CommandInterpreter.h>

#include <Hyperspace/Session.h>

#include <Hypertable/Lib/NameIdMapper.h>
#include <Hypertable/Lib/RangeServer/Client.h>
#include <Hypertable/Lib/SerializedKey.h>

#include <AsyncComm/Comm.h>

#include <Common/String.h>

#include <unordered_map>

namespace Tools {
namespace client {
namespace rangeserver {

  using namespace Lib;

  class RangeServerCommandInterpreter : public CommandInterpreter {
  public:
    RangeServerCommandInterpreter(Hyperspace::SessionPtr &,
                                  const sockaddr_in addr, RangeServer::ClientPtr &);

    int execute_line(const String &line) override;

  private:

    void display_scan_data(const SerializedKey &key, const ByteString &value,
                           SchemaPtr &schema_ptr);

    Hyperspace::SessionPtr m_hyperspace;
    NameIdMapperPtr         m_namemap;
    struct sockaddr_in m_addr;
    RangeServer::ClientPtr m_range_server;
    typedef std::unordered_map<String, TableInfo *> TableMap;
    TableMap m_table_map;
    int32_t m_cur_scanner_id;
    String m_toplevel_dir;
  };

}}}

#endif // Tools_client_rangeserver_RangeServerCommandInterpreter_h
