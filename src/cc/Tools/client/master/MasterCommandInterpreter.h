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

#ifndef Tools_client_master_MasterCommandInterpreter_h
#define Tools_client_master_MasterCommandInterpreter_h

#include <Tools/Lib/CommandInterpreter.h>

#include <Hypertable/Lib/Master/Client.h>

#include <AsyncComm/Comm.h>

#include <Common/String.h>

namespace Tools {
namespace client {
namespace master {

  class MasterCommandInterpreter : public CommandInterpreter {
  public:
    MasterCommandInterpreter(Lib::Master::ClientPtr &master);

    int execute_line(const String &line) override;

  private:
    Lib::Master::ClientPtr m_master;
  };

}}}

#endif // Tools_client_master_MasterCommandInterpreter_h
