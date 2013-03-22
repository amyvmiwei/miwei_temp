/*
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

/** @file
 * Random number generator for int32, int64, double and ascii arrays.
 */

#ifndef HYPERTABLE_RANDOM_H
#define HYPERTABLE_RANDOM_H

#include <boost/random.hpp>
#include <boost/random/uniform_01.hpp>

namespace Hypertable {

/** @addtogroup Common
 *  @{
 */

/** A random number generator using boost::mt19937 to generate int32,
 * int64, double and random ascii arrays
 */
class Random {
  public:
    /* Sets the seed of the random number generator */
    static void seed(unsigned int s) {
      ms_rng.seed((uint32_t)s);
    }

    /** Fills a buffer with random ascii values
     *
     * @param buf Pointer to the buffer
     * @param len Number of bytes to fill
     */
    static void fill_buffer_with_random_ascii(char *buf, size_t len);

    /** Fills a buffer with random values from a set of characters
     *
     * @param buf Pointer to the buffer
     * @param len Number of bytes to fill
     * @param charset A string containing the allowed characters
     */
    static void fill_buffer_with_random_chars(char *buf, size_t len,
            const char *charset);

    /** Returns a uint32 random number */
    static uint32_t number32() {
      return ms_rng();
    }

    /** Returns a uint64 random number by concatenating 2 32bit numbers */
    static int64_t number64() {
      return ((((int64_t)ms_rng()) << 32) | ((int64_t)ms_rng()));
    }

    /** Returns a random double in the interval of [0, 1]. */
    static double uniform01();

  private:
    /** The boost rng which is used under the hood */
    static boost::mt19937 ms_rng;
};

/** @} */

} // namespace Hypertable

#endif // HYPERTABLE_RANDOM_H
