/**
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

#ifndef HYPERTABLE_WORDSTREAM_H
#define HYPERTABLE_WORDSTREAM_H

#include "Common/String.h"

#include <vector>

#include <boost/random.hpp>
#include <boost/random/uniform_01.hpp>

namespace Hypertable {

  class WordStream : public ReferenceCount {
    public:

    WordStream(const String &word_file, unsigned seed,
               size_t words_per_record, bool random=false, const char *separator=" ");
    virtual ~WordStream();

    const char *next();

  private:

    struct word_info {
      const char *word;
      size_t len;
    };

    boost::mt19937 ms_rng;
    char *m_base;
    const char *m_end;
    const char *m_separator;
    size_t m_words_per_record;
    off_t m_len;
    std::vector<struct word_info> m_words;
    std::vector<size_t> m_offset;
    String m_record;
    bool m_random;
  };
  typedef intrusive_ptr<WordStream> WordStreamPtr;


}

#endif // HYPERTABLE_WORDSTREAM_H
