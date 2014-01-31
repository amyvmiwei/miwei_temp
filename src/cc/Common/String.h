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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

/** @file
 * A String class based on std::string.
 */

#ifndef HYPERTABLE_STRING_H
#define HYPERTABLE_STRING_H

#include <string>
#include <sstream>

namespace Hypertable {

  /** @addtogroup Common
   *  @{
   */

  /**
   * A String is simply a typedef to std::string.
   *
   * In the future we might want to use something better later, as std::string
   * always causes a heap allocation, and is lacking in functionalities
   * cf. http://www.and.org/vstr/comparison
   */
  typedef std::string String;

  /** Shortcut for printf formats */
  typedef long unsigned int Lu;

  /** Shortcut for printf formats */
  typedef long long unsigned int Llu;

  /** Shortcut for printf formats */
  typedef long long int Lld;

  /**
   * Returns a String using printf like format facilities
   * Vanilla snprintf is about 1.5x faster than this, which is about:
   *   10x faster than boost::format;
   *   1.5x faster than std::string append (operator+=);
   *   3.5x faster than std::string operator+;
   *
   * @param fmt A printf-like format string
   * @return A new String with the formatted text
   */
  String format(const char *fmt, ...) __attribute__((format (printf, 1, 2)));

  /**
   * Return decimal number string separated by a separator (default: comma)
   * for every 3 digits. Only 10-15% slower than sprintf("%lld", n);
   *
   * @param n The 64-bit number
   * @param sep The separator for every 3 digits
   * @return A new String with the formatted text
   */
  String format_number(int64_t n, int sep = ',');

  /**
   * Return first n bytes of buffer with an optional trailer if the
   * size of the buffer exceeds n.
   *
   * @param n The max. displayed size of the buffer
   * @param buf The memory buffer
   * @param len The size of the memory buffer
   * @param trailer Appended if %len exceeds %n
   * @return A new String with the formatted text
   */
  String format_bytes(size_t n, const void *buf, size_t len,
          const char *trailer = "...");

  /**
   * Return a string presentation of a sequence. Is quite slow but versatile,
   * as it uses ostringstream.
   *
   * @param seq A STL-compatible sequence with forward-directional iterators
   * @param sep A separator which is inserted after each list item
   * @return A new String with the formatted text
   */
  template <class SequenceT>
  String format_list(const SequenceT &seq, const char *sep = ", ") {
    typedef typename SequenceT::const_iterator Iterator;
    Iterator it = seq.begin(), end = seq.end();
    std::ostringstream out;
    out << '[';

    if (it != end) {
      out << *it;
      ++it;
    }
    for (; it != end; ++it)
      out << sep << *it;

    out << ']';
    return out.str();
  }

  /** Strips enclosing quotes.
   * Inspects the first and last characters of the string defined by
   * <code>input</code> and </code>input_len</code> and if they are either both
   * single quotes or double quotes, it sets <code>output</code> and
   * <code>output_len</code> to contained between them.  Otherwise,
   * <code>*output</code> is set to <code>input</code> and
   * <code>*output_len</code> is set to <code>input_len</code>.
   * @param input Input char string
   * @param input_len Input char string length
   * @param output Address of output char string pointer
   * @param output_len Address of output char string pointer
   * @return <i>true</i> if quotes were stripped, <i>false</i> otherwise.
   */
  inline bool strip_enclosing_quotes(const char *input, size_t input_len,
                                     const char **output, size_t *output_len) {
    if (input_len < 2 ||
        !((*input == '\'' && input[input_len-1] == '\'') ||
          (*input == '"' && input[input_len-1] == '"'))) {
      *output = input;
      *output_len = input_len;
      return false;
    }
    *output = input + 1;
    *output_len = input_len - 2;
    return true;
  }

  /** @} */

} // namespace Hypertable

#endif // HYPERTABLE_STRING_H
