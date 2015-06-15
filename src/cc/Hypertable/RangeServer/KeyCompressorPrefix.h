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

#ifndef Hypertable_RangeServer_KeyCompressorPrefix_h
#define Hypertable_RangeServer_KeyCompressorPrefix_h

#include "KeyCompressor.h"

namespace Hypertable {

  class KeyCompressorPrefix : public KeyCompressor {
  public:
    virtual void reset();
    virtual void add(const Key &key);
    virtual size_t length();
    virtual size_t length_uncompressed();
    virtual void write(uint8_t *buf);
    virtual void write_uncompressed(uint8_t *buf);
  private:
    void render_uncompressed();
    DynamicBuffer m_buffer;
    DynamicBuffer m_compressed_key;
    DynamicBuffer m_uncompressed_key;
    uint8_t      *m_suffix;
    size_t        m_suffix_length;
    uint8_t       m_last_control;
  };

}

#endif // Hypertable_RangeServer_KeyCompressorPrefix_h
