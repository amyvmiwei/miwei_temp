/*
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

/** @file
 * Definition of MetricsCollectorGanglia.
 * This file contains the definition of MetricsCollectorGanglia, a class for
 * collection and publishing of Ganglia metrics.
 */

package org.hypertable.Common;

import java.lang.Double;
import java.lang.Integer;
import java.lang.String;
import java.lang.System;
import java.net.DatagramSocket;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.PortUnreachableException;
import java.util.HashMap;
import java.util.Map;
import java.net.UnknownHostException;
import java.net.SocketException;


public class MetricsCollectorGanglia implements MetricsCollector {

  public MetricsCollectorGanglia(String component, short port) {
    mPrefix = "hypertable." + component + ".";
    mPort = port;

    try {
      mAddr = InetAddress.getByName("localhost");
    }
    catch (UnknownHostException e) {
      System.out.println("UnknownHostException : 'localhost'");
      e.printStackTrace();
      System.exit(-1);
    }

    try {
      mSocket = new DatagramSocket();
    }
    catch (SocketException e) {
      e.printStackTrace();
      System.exit(-1);
    }

    mSocket.connect(mAddr, mPort);
    mConnected = true;
  }

  public void update(String name, String value) {
    mValuesString.put(mPrefix+name, value);
  }

  public void update(String name, short value) {
    mValuesInt.put(mPrefix+name, new Integer((int)value));
  }

  public void update(String name, int value) {
    mValuesInt.put(mPrefix+name, new Integer(value));
  }

  public void update(String name, float value) {
    mValuesDouble.put(mPrefix+name, new Double((double)value));
  }

  public void update(String name, double value) {
    mValuesDouble.put(mPrefix+name, new Double(value));
  }

  public void publish() throws Exception {

    if (!mConnected)
      mSocket.connect(mAddr, mPort);

    boolean first = true;
    String message = "{ ";

    for (Map.Entry<String, String> entry : mValuesString.entrySet()) {
      if (!first)
        message += ", \"";
      else {
        message += "\"";
        first = false;
      }
      message += entry.getKey() + "\": \"" + entry.getValue() + "\"";
    }

    for (Map.Entry<String, Integer> entry : mValuesInt.entrySet()) {
      if (!first)
        message += ", \"";
      else {
        message += "\"";
        first = false;
      }
      message += entry.getKey() + "\": " + entry.getValue();
    }

    for (Map.Entry<String, Double> entry : mValuesDouble.entrySet()) {
      if (!first)
        message += ", \"";
      else {
        message += "\"";
        first = false;
      }
      message += entry.getKey() + "\": " + entry.getValue();
    }

    message += " }";

    mConnected = false;
    byte [] sendData = message.getBytes("UTF-8");
    DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, mAddr, mPort);
    mSocket.send(sendPacket);
    mConnected = true;

  }

  private String mPrefix;
  private InetAddress mAddr;
  private short mPort;
  private boolean mConnected = false;
  private DatagramSocket mSocket;
  private HashMap<String, String> mValuesString = new HashMap<String, String>();
  private HashMap<String, Integer> mValuesInt = new HashMap<String, Integer>();
  private HashMap<String, Double> mValuesDouble = new HashMap<String, Double>();
}
