/** -*- C++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License.
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
#include "Config.h"

namespace Hypertable { namespace Config {

void init_fs_client_options() {
  cmdline_desc().add_options()
    ("fs", str(),
        "FS client endpoint in <host:port> format")
    ("fs-timeout", i32(),
        "Timeout in milliseconds for FS client connections")
    ;
  alias("fs-timeout", "FsBroker.Timeout");
  // hidden aliases
  alias("fs-host", "FsBroker.Host");
  alias("fs-port", "FsBroker.Port");
}

void init_fs_client() {
  // prepare hidden aliases to be synced
  if (properties->has("fs")) {
    Endpoint e = InetAddr::parse_endpoint(get_str("fs"));
    bool defaulted = properties->defaulted("fs");
    properties->set("fs-host", e.host, defaulted);
    properties->set("fs-port", e.port, !e.port || defaulted);
  }
}

void init_fs_broker_options() {
  cmdline_desc().add_options()
    ("port", i16(), "Listening port")
    ("pidfile", str(), "File to contain the process id")
    ;
}

}} // namespace Hypertable::Config
