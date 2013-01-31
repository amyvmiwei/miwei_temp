/** -*- C -*-
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#include "Common/Compat.h"

#include "AsyncComm/Comm.h"

#include "TableScannerHandler.h"
#include "TableScannerAsync.h"

using namespace Hypertable;

void TableScannerHandler::run() {
  int32_t error;
  try {
    switch (m_event->type) {
    case(Event::MESSAGE):
      error = Protocol::response_code(m_event);
      if (error == Error::REQUEST_TIMEOUT)
        m_scanner->handle_timeout(m_interval_scanner, m_event->to_str(), m_is_create);
      else if (error != Error::OK)
        m_scanner->handle_error(m_interval_scanner, error, m_event->to_str(), m_is_create);
      else
        m_scanner->handle_result(m_interval_scanner, m_event, m_is_create);
      break;
    case(Event::TIMER):
      m_scanner->handle_timeout(m_interval_scanner, m_event->to_str(), m_is_create);
      break;
    case(Event::ERROR):
      m_scanner->handle_error(m_interval_scanner, m_event->error, m_event->to_str(),
          m_is_create);
      break;
    default:
      HT_INFO_OUT << "Received event " << m_event->to_str() << HT_END;
    }
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    m_scanner->cancel();
  }
}
