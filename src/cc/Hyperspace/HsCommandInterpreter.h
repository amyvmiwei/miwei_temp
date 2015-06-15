/* -*- c++ -*-
 * Copyright (C) 2007-2015 Hypertable, Inc.
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

#ifndef Hyperspace_HsCommandInterpreter_h
#define Hyperspace_HsCommandInterpreter_h

#include "DirEntryAttr.h"
#include "HsClientState.h"

#include <Tools/Lib/CommandInterpreter.h>

#include <Common/String.h>

#include <memory>
#include <vector>

namespace Hyperspace {

  using namespace std;

  class Session;

  class HsCommandInterpreter : public CommandInterpreter {
  public:
    HsCommandInterpreter(Session* session);

    int execute_line(const String &line) override;

  private:
    static void printDirEntryAttrListing(int indent, const String& attr_name, const std::vector<DirEntryAttr> listing);

    Session* m_session;
  };

  typedef std::shared_ptr<HsCommandInterpreter> HsCommandInterpreterPtr;

}

#endif // Hyperspace_HsCommandInterpreter_h
