/* -*- c++ -*-
 * Copyright (C) 2007-2013 Hypertable, Inc.
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

/** @file
 * Declarations for NotificationMessage
 * This file contains declarations for NotificationMessage, a class for holding
 * a notification message to be delivered to the administrator.
 */

#ifndef HYPERTABLE_NOTIFICATIONMESSAGE_H
#define HYPERTABLE_NOTIFICATIONMESSAGE_H

#include "Common/Serialization.h"

namespace Hypertable {

  /** @addtogroup Master
   *  @{
   */

  /** Holds a notification message to be deliverd to the administrator */
  class NotificationMessage {
  public:

    /** Constructor.
     * @param s Message subject
     * @param b Message body
     */
    NotificationMessage(const String &s, const String &b)
      : subject(s), body(b) { }

    /// Message subject
    String subject;

    /// Message body
    String body;
  };

  /* @}*/

} // namespace Hypertable

#endif // HYPERTABLE_NOTIFICATIONMESSAGE_H
