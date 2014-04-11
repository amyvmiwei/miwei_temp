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

package org.hypertable.FsBroker.hadoop;

import java.net.ProtocolException;
import java.util.logging.Logger;
import org.hypertable.AsyncComm.ApplicationHandler;
import org.hypertable.AsyncComm.Comm;
import org.hypertable.AsyncComm.Event;
import org.hypertable.AsyncComm.ResponseCallback;
import org.hypertable.Common.Error;

public class RequestHandlerSeek extends ApplicationHandler {

    static final Logger log = Logger.getLogger(
        "org.hypertable.FsBroker.hadoop");

    public RequestHandlerSeek(Comm comm, HadoopBroker broker, Event event) {
        super(event);
        mComm = comm;
        mBroker = broker;
    }

    public void run() {
        int   fd;
        long  offset;
        ResponseCallback cb = new ResponseCallback(mComm, mEvent);

        try {

            if (mEvent.payload.remaining() < 12)
                throw new ProtocolException("Truncated message");

            fd = mEvent.payload.getInt();
            offset = mEvent.payload.getLong();

            mBroker.Seek(cb, fd, offset);

        }
        catch (ProtocolException e) {
            int error = cb.error(Error.PROTOCOL_ERROR, e.getMessage());
            log.severe("Protocol error (SEEK) - " + e.getMessage());
            if (error != Error.OK)
                log.severe("Problem sending (SEEK) error back to client - "
                           + Error.GetText(error));
        }
    }

    private Comm       mComm;
    private HadoopBroker mBroker;
}
