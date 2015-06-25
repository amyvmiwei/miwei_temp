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

#ifndef Hypertable_Lib_DataGeneratorRandom_h
#define Hypertable_Lib_DataGeneratorRandom_h

#include <string>

namespace Hypertable {

  /// @addtogroup Common
  /// @{

  /// Generate random 32-bit integer
  /// @return Random integer within range [0,<code>maximum</code>)
  extern int32_t random_int32(int32_t maximum);

  /// Sets random number generator seed
  /// @param seed Random number generator seed
  extern void random_generator_set_seed(unsigned seed);

  /// Fills a buffer with random values from a set of characters.
  /// @param buf Pointer to the buffer
  /// @param len Number of bytes to fill
  /// @param charset A string containing the allowed characters
  extern void random_fill_with_chars(char *buf, size_t len,
                                     const char *charset = nullptr);

  /// @}

}

#endif // Hypertable_Lib_DataGeneratorRandom_h
