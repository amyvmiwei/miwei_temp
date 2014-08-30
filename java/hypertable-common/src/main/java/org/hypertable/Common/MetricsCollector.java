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
 * Definition of MetricsCollector interface.
 * This file contains the interface definition of MetricsCollector, an interface for
 * metrics collector classes.
 */

package org.hypertable.Common;

import java.lang.String;

public interface MetricsCollector {
  public void update(String name, String value);
  public void update(String name, short value);
  public void update(String name, int value);
  public void update(String name, float value);
  public void update(String name, double value);
  public void publish();
}
