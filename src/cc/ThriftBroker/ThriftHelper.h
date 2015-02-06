/* -*- C++ -*-
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#ifndef Hypertable_ThriftBroker_ThriftHelper_h
#define Hypertable_ThriftBroker_ThriftHelper_h

#include <iosfwd>
#include "gen-cpp/HqlService.h"

namespace Hypertable { namespace ThriftGen {

std::ostream &operator<<(std::ostream &, const CellAsArray &);

// These are mostly for test code and not efficient for production.
Cell
make_cell(const char *row, const char *cf, const char *cq,
          const std::string &value, int64_t ts, int64_t rev, KeyFlag::type flag);

Cell
make_cell(const char *row, const char *cf, const char *cq = 0,
          const std::string &value = std::string(), const char *ts = 0,
          const char *rev = 0, KeyFlag::type flag = KeyFlag::INSERT);

}}

#endif // Hypertable_ThriftBroker_ThriftHelper_h
