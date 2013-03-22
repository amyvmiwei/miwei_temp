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
 * A class generating a stream of words; the words are retrieved from a file
 * and can be randomized.
 */

#ifndef HYPERTABLE_WORDSTREAM_H
#define HYPERTABLE_WORDSTREAM_H

#include "Common/String.h"

#include <vector>

#include <boost/random.hpp>
#include <boost/random/uniform_01.hpp>

namespace Hypertable {

  /** @addtogroup Common
   *  @{
   */

  /**
   * A class generating a stream of words; the words are retrieved from a file
   * and can be randomized.
   */
  class WordStream : public ReferenceCount {
    public:
    /** Constructor
     *
     * @param word_file The file with all the words
     * @param seed The seed for the random number generator; if 0 then no seed
     *          will be set
     * @param words_per_record Number of words to retrieve when calling %next
     * @param random If true, words will be in random order
     * @param separator The word separator
     */
    WordStream(const String &word_file, unsigned seed, size_t words_per_record,
            bool random = false, const char *separator = " ");

    /**
     * Releases internal resources
     */
    virtual ~WordStream();

    /** Retrieves the next word, or an empty string if EOF is reached */
    const char *next();

  private:
    /** Internal structure for a single word */
    struct word_info {
      const char *word;
      size_t len;
    };

    /** Random number generator */
    boost::mt19937 ms_rng;

    /** Base pointer for the memory mapped file */
    char *m_base;

    /** End pointer for the memory mapped file */
    const char *m_end;

    /** The separator, as specified by the user */
    const char *m_separator;

    /** Words per record, as specified by the user */
    size_t m_words_per_record;

    /** Length of the memory mapped file */
    off_t m_len;

    /** All words from the mapped file */
    std::vector<struct word_info> m_words;

    /** Helper for parsing the words */
    std::vector<size_t> m_offset;

    /** The current string */
    String m_record;

    /** Whether to return random strings or not */
    bool m_random;
  };

  typedef intrusive_ptr<WordStream> WordStreamPtr;

  /** @} */

}

#endif // HYPERTABLE_WORDSTREAM_H
