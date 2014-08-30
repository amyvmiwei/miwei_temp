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
 * Declarations for MetricsHandler.
 * This file contains declarations for MetricsHandler, a dispatch handler class
 * used to collect and publish FsBroker metrics.
 */

package org.hypertable.FsBroker.Lib;

import java.lang.Integer;
import java.lang.Short;
import java.util.Properties;
import org.hypertable.AsyncComm.Comm;
import org.hypertable.AsyncComm.DispatchHandler;
import org.hypertable.AsyncComm.Event;
import org.hypertable.Common.MetricsProcess;

/** Collects and publishes %Hyperspace metrics.
 * This class acts as the timer dispatch handler for periodic metrics
 * collection for Hyperspace.
 */
public class MetricsHandler implements DispatchHandler {

  /** Constructor.
   * Initializes #m_collection_interval to the property
   * <code>Hypertable.Monitoring.Interval</code> and allocates a Ganglia
   * collector object, initializing it with "hyperspace" and
   * <code>Hypertable.Metrics.Ganglia.Port</code>.  Lastly, calls
   * Comm::set_timer() to register a timer for
   * #m_collection_interval milliseconds in the future and passes
   * <code>this</code> as the timer handler.
   * @param props %Properties object
   */
  public MetricsHandler(Comm comm, Properties props) {
    String str;

    str = props.getProperty("Hypertable.Metrics.Ganglia.Port", "15860");
    short port = Short.parseShort(str);

    str = props.getProperty("Hypertable.Monitoring.Interval", "30000");
    mCollectionInterval = Integer.parseInt(str);

    mComm = comm;

    mComm.SetTimer(mCollectionInterval, this);
  }

  /** Collects and publishes metrics.
   * This method updates the <code>requests/s</code> and general process
   * metrics and publishes them via #m_ganglia_collector.  After metrics have
   * been collected, the timer is re-registered for #m_collection_interval
   * milliseconds in the future.
   * @param event %Comm layer timer event
   */
  public void handle(Event event) {

    if (event.type == Event.Type.TIMER) {
      mMetricsProcess.collect(0, null);
    }
    else {
      System.out.println("Unexpected event - " + event);
      System.exit(-1);
    }

    mComm.SetTimer(mCollectionInterval, this);    
  }

  /** Metrics collection interval */
  private int mCollectionInterval = 0;

  /** Comm layer object */
  private Comm mComm;

  /** Process metrics tracker */
  private MetricsProcess mMetricsProcess = new MetricsProcess();

}
