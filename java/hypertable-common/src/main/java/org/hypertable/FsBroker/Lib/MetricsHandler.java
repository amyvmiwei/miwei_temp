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
import java.util.logging.Logger;
import org.hypertable.AsyncComm.Comm;
import org.hypertable.AsyncComm.DispatchHandler;
import org.hypertable.AsyncComm.Event;
import org.hypertable.Common.MetricsCollectorGanglia;
import org.hypertable.Common.MetricsProcess;

/** Collects and publishes %Hyperspace metrics.
 * This class acts as the timer dispatch handler for periodic metrics
 * collection for Hyperspace.
 */
public class MetricsHandler implements DispatchHandler {

  static final Logger log = Logger.getLogger("org.hypertable.FsBroker.Lib");

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

    mMetricsCollectorGanglia = new MetricsCollectorGanglia("fsbroker", port);
    mMetricsCollectorGanglia.update("type", "hadoop");

    str = props.getProperty("Hypertable.Monitoring.Interval", "30000");
    mCollectionInterval = Integer.parseInt(str);

    mComm = comm;

    mLastTimestamp = System.currentTimeMillis();

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
      long now = System.currentTimeMillis();
      long elapsed_millis = now - mLastTimestamp;
      long elapsed_seconds = elapsed_millis / 1000;
      mMetricsProcess.collect(now, mMetricsCollectorGanglia);
      mMetricsCollectorGanglia.update("type", "hadoop");
      synchronized (this) {
        mMetricsCollectorGanglia.update("errors", mErrors);
        double sps = (double)mSyncs / (double)elapsed_seconds;
        mMetricsCollectorGanglia.update("syncs", sps);
        if (mSyncs > 0)
          mMetricsCollectorGanglia.update("syncLatency", mSyncLatency/mSyncs);
        if (elapsed_millis > 0) {
          long mbps = (mBytesRead / 1000000) / elapsed_seconds;
          mMetricsCollectorGanglia.update("readThroughput", (int)mbps);
          mbps = (mBytesWritten / 1000000) / elapsed_seconds;
          mMetricsCollectorGanglia.update("writeThroughput", (int)mbps);
        }
        mLastTimestamp = now;
        mErrors = 0;
        mSyncs = 0;
        mSyncLatency = 0;
        mBytesRead = 0;
        mBytesWritten = 0;
      }
      try {
        mMetricsCollectorGanglia.publish();
      }
      catch (Exception e) {
        log.info("Problem publishing Ganglia metrics - " +
                 e.getMessage());
      }
    }
    else {
      System.out.println("Unexpected event - " + event);
      System.exit(-1);
    }

    mComm.SetTimer(mCollectionInterval, this);    
  }

  public synchronized void addBytesRead(long count) {
    mBytesRead += count;
  }

  public synchronized void addBytesWritten(long count) {
    mBytesWritten += count;
  }

  public synchronized void addSync(long latency) {
    mSyncs++;
    mSyncLatency += (int)latency;
  }

  public synchronized void incrementErrorCount() {
    mErrors++;
  }

  /** Metrics collection interval */
  private int mCollectionInterval = 0;

  /** Comm layer object */
  private Comm mComm;

  /** Process metrics tracker */
  private MetricsProcess mMetricsProcess = new MetricsProcess();

  /** Ganglia metrics collector */
  private MetricsCollectorGanglia mMetricsCollectorGanglia;

  /** Timestamp (ms) of last metrics collection */
  private long mLastTimestamp;

  /** Bytes written since last metrics collection */
  private long mBytesWritten = 0;

  /** Bytes read since last metrics collection */
  private long mBytesRead = 0;

  /** Cumulative sync latency since last metrics collection */
  private int mSyncLatency = 0;

  /** Syncs since last metrics collection */
  private int mSyncs = 0;

  /** Error count since last metrics collection */
  private long mErrors = 0;

}
