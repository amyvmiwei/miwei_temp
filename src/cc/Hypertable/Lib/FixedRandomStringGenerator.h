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

#ifndef Hypertable_Lib_FixedRandomStringGenerator_h
#define Hypertable_Lib_FixedRandomStringGenerator_h

#include <cstdint>
#include <cstring>
#include <vector>

namespace Hypertable {

  class FixedRandomStringGenerator {
  public:
    FixedRandomStringGenerator(int n);
    void write(char *buf);
  private:
    size_t m_nchars;
    size_t m_nints;
    std::vector<uint32_t> m_ivec;
  };

}

#endif // Hypertable_Lib_FixedRandomStringGenerator_h
