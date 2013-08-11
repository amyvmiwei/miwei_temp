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
 * Declarations for SystemState.
 * This file contains declarations for SystemState, a MetaLog entity
 * for persisting global system state.
 */

#ifndef HYPERTABLE_SYSTEMSTATE_H
#define HYPERTABLE_SYSTEMSTATE_H

#include <vector>

#include "Hypertable/Lib/MetaLogEntity.h"
#include "Hypertable/Lib/SystemVariable.h"

#include "NotificationMessage.h"

namespace Hypertable {

  /** @addtogroup Master
   *  @{
   */

  /** Holds persistent global system state. */
  class SystemState : public MetaLog::Entity {
  public:

    /** Constructor.
     * This constructor intializes the #m_admin_specified and
     * #m_auto_specified arrays with system variables and their
     * default values.  It also initializes #m_admin_last_notification
     * and #m_auto_last_notification to the same size as the spec arrays, with
     * all zeros.  It also sets #m_notification_interval to the value of the
     * <code>Hypertable.Master.NotificationInterval</code> property.
     */
    SystemState();

    /** Constructor from MetaLog entity.
     * @param header_ MetaLog entity header
     */
    SystemState(const MetaLog::EntityHeader &header_);

    /** Destructor. */
    virtual ~SystemState() { }

    /** Set a vector of variables by administrator.
     * For each variable spec in <code>specs</code>, this method updates
     * the values in the #m_admin_specified vector.  If the variable state
     * changes, or the variable is in the non-default state and the time
     * since the last notification has exceeded #m_notification_interval,
     * it creates a notification and adds it to #m_notifications.
     * @param specs Vector of variable specifications
     * @return <i>true</i> if variable state changed and generation
     * number incremented, <i>false</i> otherwise.
     */
    bool admin_set(std::vector<SystemVariable::Spec> &specs);

    /** Set a variable by administrator.
     * This method updates the values in the #m_admin_specified vector.  If the
     * variable state changes, or the variable is in the non-default state and
     * the time since the last notification has exceeded
     * #m_notification_interval, it creates a notification and adds it to
     * #m_notifications.
     * @param code Variable code to set
     * @param value Value of variable to set
     * @return <i>true</i> if variable state changed and generation
     * number incremented, <i>false</i> otherwise.
     */
    bool admin_set(int code, bool value);

    /** Set a variable by automated condition.
     * This method updates the values in the #m_auto_specified vector.  If the
     * variable state changes, or the variable is in the non-default state and
     * the time since the last notification has exceeded
     * #m_notification_interval, it creates a notification and adds it to
     * #m_notifications.  The notification message body is constructed as:
     * 
     *   - System state VARIABLE=<code>value</code> <code>reason</code>.
     *
     * @param code Variable code to set
     * @param value Value of variable to set
     * @param reason String describing reason for state change
     * @return <i>true</i> if variable state changed and generation
     * number incremented, <i>false</i> otherwise.
     */
    bool auto_set(int code, bool value, const String &reason);

    /** Get system variables and generation number.
     * This method returns a vector of system variables and their current values
     * as well as the current system state generation number.  Each variable's
     * value is set to the default value if both the administratively and
     * automatically set variables are set to the default value, otherwise the
     * value is set to boolean NOT of the default value.
     * @param specs Reference to vector of variable specifications
     * @param generation Address of variable to hold generation number
     */
    void get(std::vector<SystemVariable::Spec> &specs, uint64_t *generation);

    /** Get pending notifications.
     * This method copies #m_notifications to <code>notifications</code> and
     * then clears #m_notifications.
     * @return <i>true</i> if no pending notifications, <i>false</i> otherwise.
     */
    bool get_notifications(std::vector<NotificationMessage> &notifications);

    /** Get system state variables that are not set to their default value.
     * This method returns a vector of system variable specs for the variables
     * that are not set to their default value.
     * @param specs Reference to vector of variable specifications
     * @param generation Address of variable to hold generation number
     */
    void get_non_default(std::vector<SystemVariable::Spec> &specs,
                         uint64_t *generation=0);

    /** Return name of entity.
     * @return Name of entity.
     */
    virtual const String name() { return "SystemState"; }

    /** Return textual representation of entity state.
     * @param os Output stream on which to write state string
     */
    virtual void display(std::ostream &os);

    /** Returns length of encoded state. */
    virtual size_t encoded_length() const;

    /** Encodes system state.
     * This method encodes the system state formatted as follows:
     * <pre>
     * Format version number
     * System state generation number
     * Number of administratively set variable specs
     * foreach administratively set variable spec {
     *   variable code (i32)
     *   variable value (bool)
     *   time (seconds since epoch) of last notification (i32)
     * }
     * Number of automatically set variable specs
     * foreach automatically set variable spec {
     *   variable code (i32)
     *   variable value (bool)
     *   time (seconds since epoch) of last notification (i32)
     * }
     * </pre>
     * @param bufp Address of destination buffer pointer (advanced by call)
     */
    virtual void encode(uint8_t **bufp) const;

    /** Decodes system state.
     * See encode_state() for format description.
     * @param bufp Address of destination buffer pointer (advanced by call)
     * @param remainp Address of integer holding amount of remaining buffer
     */
    virtual void decode(const uint8_t **bufp, size_t *remainp);

  private:

    /// Generation number incremented with each state change
    uint64_t m_generation;

    /// Administratively set state variables
    std::vector<SystemVariable::Spec> m_admin_specified;

    /// Last notification times for admin set variables
    std::vector<int32_t> m_admin_last_notification;

    /// Automatically set state variables
    std::vector<SystemVariable::Spec> m_auto_specified;

    /// Last notification times for auto set variables
    std::vector<int32_t> m_auto_last_notification;

    /// Pending notification messages
    std::vector<NotificationMessage> m_notifications;

    /// Notification interval in seconds
    int32_t m_notification_interval;
  };

  /// Smart pointer to SystemState
  typedef intrusive_ptr<SystemState> SystemStatePtr;

  /** @}*/

} // namespace Hypertable

#endif // HYPERTABLE_SYSTEMSTATE_H
