/**
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

#include "Common/Compat.h"
#include "Common/InetAddr.h"
#include "Tools/Lib/CommandShell.h"
#include "Config.h"

namespace Hypertable { namespace Config {

void init_command_shell_options() {
  CommandShell::add_options(cmdline_desc());
}

void init_master_client_options() {
  cmdline_desc().add_options()
    ("master", str(),
        "master server to connect in <host:port> format")
    ;
  // hidden aliases
  alias("master-host", "Hypertable.Master.Host");
  alias("master-port", "Hypertable.Master.Port");
}

void init_master_client() {
  if (properties->has("master")) {
    Endpoint e = InetAddr::parse_endpoint(get_str("master"));
    bool defaulted = properties->defaulted("master");
    properties->set("master-host", e.host, defaulted);
    properties->set("master-port", e.port, !e.port || defaulted);
  }
  else
    properties->set("master-host", String("localhost"));
}

void init_range_server_client_options() {
  cmdline_desc().add_options()
    ("range-server", str(),
        "range server to connect in <host:port> format")
    ;
  // hidden aliases
  alias("rs-port", "Hypertable.RangeServer.Port");
}

void init_range_server_client() {
  if (properties->has("range-server")) {
    Endpoint e = InetAddr::parse_endpoint(get_str("range-server"));
    bool defaulted = properties->defaulted("range-server");
    properties->set("rs-host", e.host, defaulted);
    properties->set("rs-port", e.port, !e.port || defaulted);
  }
  else
    properties->set("rs-host", String("localhost"));
}

}} // namespace Hypertable::Config
