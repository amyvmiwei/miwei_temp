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

import org.hypertable.AsyncComm.Comm;
import org.hypertable.AsyncComm.CommBuf;
import org.hypertable.AsyncComm.CommHeader;
import org.hypertable.AsyncComm.Event;
import org.hypertable.AsyncComm.ResponseCallback;
import org.hypertable.AsyncComm.Serialization;
import org.hypertable.Common.Error;

public class ResponseCallbackPosixReaddir extends ResponseCallback {

    ResponseCallbackPosixReaddir(Comm comm, Event event) {
        super(comm, event);
    }

    int response(HadoopBroker.DirectoryEntry [] listing) {
        CommHeader header = new CommHeader();
        int listing_count = 0;
        header.initialize_from_request_header(mEvent.header);
        if (listing != null)
            listing_count = listing.length;
        int len = 8;
        for (int i = 0; i < listing_count; i++)
            len += 4 + 4 + Serialization.EncodedLengthString(listing[i].name);

        CommBuf cbuf = new CommBuf(header, len);
        cbuf.AppendInt(Error.OK);
        cbuf.AppendInt(listing_count);
        for (int i = 0; i < listing_count; i++) {
            cbuf.AppendString(listing[i].name);
            cbuf.AppendInt(listing[i].flags);
            cbuf.AppendInt(listing[i].length);
        }
        return mComm.SendResponse(mEvent.addr, cbuf);
    }
}

