/* -*- c++ -*-
 * Copyright (C) 2007-2014 Hypertable, Inc.
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

#ifndef Tools_cluster_ClusterCommandInterpreter_h
#define Tools_cluster_ClusterCommandInterpreter_h

#include <Tools/Lib/CommandInterpreter.h>

#include <string>

namespace Hypertable {

  using namespace std;

  class ClusterCommandInterpreter : public CommandInterpreter {
  public:

    /// Constructor.
    /// @param script Absolute pathname of command script
    ClusterCommandInterpreter(const string &script);

    virtual void execute_line(const string &line);

  private:

    /// Pathname of command script
    string m_command_script;

  };

}

#endif // Tools_cluster_ClusterCommandInterpreter_h
