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
#include "Common/Compat.h"

#include <cctype>

extern "C" {
#include <sys/mman.h>
}

#include "FileUtils.h"
#include "Properties.h"

#include "WordStream.h"

using namespace Hypertable;

#define SKIP_SPACE \
  while (ptr < m_end && isspace(*ptr)) \
    ptr++;

WordStream::WordStream(const String &word_file, unsigned seed,
                       size_t words_per_record, bool random, const char *separator)
  : m_separator(separator), m_words_per_record(words_per_record), m_random(random) {

  ms_rng.seed((uint32_t)seed);

  if (!m_random)
    m_offset.resize(words_per_record, 0);

  m_base = (char *)FileUtils::mmap(word_file, &m_len);
  m_end = m_base + m_len;
    
  size_t count = 0;
    
  for (const char *ptr = m_base; ptr < m_end; ++ptr) {
    if (*ptr == '\n')
      count++;
  }

  m_words.reserve(count);

  const char *ptr = m_base;
  struct word_info wi;
  SKIP_SPACE;
  wi.word = ptr;
  for (; ptr < m_end; ++ptr) {
    if (isspace(*ptr)) {
      count++;
      wi.len = ptr-wi.word;
      m_words.push_back(wi);
      SKIP_SPACE;
      wi.word = ptr;
    }
  }
}


WordStream::~WordStream() {
  ::munmap((void *)m_base, m_len);
}


const char *WordStream::next() {
  size_t offset;
  m_record.clear();

  for (size_t i=0; i<m_words_per_record; ++i) {
    if (m_random) {
      offset = ms_rng() % m_words.size();
    }
    else {
      if (m_offset[i] == m_words.size()) {
        m_offset[i] = offset = 0;
        if (i < (m_words_per_record-1))
          m_offset[i+1]++;
      }
      else if (i == 0)
        offset = m_offset[i]++;
      else
        offset = m_offset[i];
    }
    m_record += String(m_words[offset].word, m_words[offset].len) + m_separator;
  }

  m_record.resize(m_record.size()-1);
  return m_record.c_str();
}


