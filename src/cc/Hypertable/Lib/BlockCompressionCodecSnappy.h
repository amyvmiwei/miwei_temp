/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

#ifndef HYPERTABLE_BLOCKCOMPRESSIONCODECSNAPPY_H
#define HYPERTABLE_BLOCKCOMPRESSIONCODECSNAPPY_H

#include <snappy.h>

#include "BlockCompressionCodec.h"

namespace Hypertable {

  class BlockCompressionCodecSnappy : public BlockCompressionCodec {

  public:
    BlockCompressionCodecSnappy(const Args &args);
    virtual ~BlockCompressionCodecSnappy();

    virtual void set_args(const Args &args);
    virtual void deflate(const DynamicBuffer &input, DynamicBuffer &output,
                         BlockCompressionHeader &header, size_t reserve=0);
    virtual void inflate(const DynamicBuffer &input, DynamicBuffer &output,
                         BlockCompressionHeader &header);
    virtual int get_type() { return SNAPPY; }

  private:
  };

}

#endif // HYPERTABLE_BLOCKCOMPRESSIONCODECSNAPPY_H

