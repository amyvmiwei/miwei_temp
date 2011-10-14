/**
 * Copyright (C) 2010 Doug Judd (Hypertable, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
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

package org.hypertable.examples.PerformanceTest;

import java.nio.ByteBuffer;

import org.hypertable.AsyncComm.Serialization;

public class MessageSetup extends Message {

  public MessageSetup() {
    super(Message.Type.SETUP);
  }

  public MessageSetup(String name, String driver, String valueData, Task.Type tt, int parallelism) {
    super(Message.Type.SETUP);
    mTableName = name;
    mDriver = driver;
    mValueData = valueData;
    mTestType = tt;
    mParallelism = parallelism;
  }

  public int encodedLength() {
    return Serialization.EncodedLengthString(mTableName) + 
      Serialization.EncodedLengthString(mDriver) + Serialization.EncodedLengthString(mValueData) + 8;
  }
  public void encode(ByteBuffer buf) {
    Serialization.EncodeString(buf, mTableName);
    Serialization.EncodeString(buf, mDriver);
    Serialization.EncodeString(buf, mValueData);
    buf.putInt(mTestType.ordinal());
    buf.putInt(mParallelism);
  }
  public void decode(ByteBuffer buf) {
    mTableName = Serialization.DecodeString(buf);
    mDriver = Serialization.DecodeString(buf);
    mValueData = Serialization.DecodeString(buf);
    mTestType = Task.Type.values()[buf.getInt()];
    mParallelism = buf.getInt();
  }

  public String toString() {
    return new String("MESSAGE:SETUP { table=" + mTableName + ", driver=" + mDriver + ", valueData=" + mValueData + ", type=" + mTestType + ", parallelism=" + mParallelism + "}");
  }

  public void setTableName(String name) { mTableName = name; }
  public String getTableName() { return mTableName; }

  public void setDriver(String name) { mDriver = name; }
  public String getDriver() { return mDriver; }

  public void setValueData(String name) { mValueData = name; }
  public String getValueData() { return mValueData; }

  public void setTestType(Task.Type t) { mTestType = t; }
  public Task.Type getTestType() { return mTestType; }

  public void setParallelism(int p) { mParallelism = p; }
  public int getParallelism() { return mParallelism; }

  private String mTableName;
  private String mDriver;
  private String mValueData;
  private Task.Type mTestType;
  private int mParallelism;
}
