/**
 * Copyright (C) 2007-2014 Hypertable, Inc.
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

package org.hypertable.Common;

import org.hypertable.AsyncComm.Serialization;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Filesystem {

  /**
   * A directory entry for readdir.
   */
  public static class Dirent {
    public int EncodedLength() {
      return Serialization.EncodedLengthString(name) + 13;
    }
    public void Encode(ByteBuffer buf) {
      buf.order(ByteOrder.LITTLE_ENDIAN);
      Serialization.EncodeString(buf, name);
      buf.putLong(length);
      buf.putInt(last_modification_time);
      buf.put(is_dir ? (byte)1 : (byte)0);
    }
    public void Decode(ByteBuffer buf) {
      buf.order(ByteOrder.LITTLE_ENDIAN);
      name = Serialization.DecodeString(buf);
      length = buf.getLong();
      last_modification_time = buf.getInt();
      is_dir = buf.get() == (byte)0 ? false : true;
    }
    public String name;
    public long length = 0;
    public int last_modification_time = 0;
    public boolean is_dir = false;
  }

}
