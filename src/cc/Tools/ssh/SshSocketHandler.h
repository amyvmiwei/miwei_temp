/* -*- c++ -*-
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

/// @file
/// Declarations for SshSocketHandler.
/// This file contains type declarations for SshSocketHandler, a raw socket
/// handler for implementing an ssh protocol driver.

#ifndef Tools_cluster_SshSocketHandler_h
#define Tools_cluster_SshSocketHandler_h

#include "SshOutputCollector.h"

#include <AsyncComm/Comm.h>
#include <AsyncComm/CommAddress.h>
#include <AsyncComm/RawSocketHandler.h>

#include <Common/PageArena.h>

#include <libssh/callbacks.h>
#include <libssh/libssh.h> 

#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <tuple>

namespace Hypertable {

  /// @addtogroup ssh
  /// @{

  /// Raw socket handler for ssh protocol driver.
  class SshSocketHandler : public RawSocketHandler {
  public:

    /// Constructor.
    /// Initializes handler by creating a non-blocking TCP socket and
    /// asynchonously connecting to <code>hostname</code>.
    /// @param hostname Name of host to connect to
    SshSocketHandler(const std::string &hostname);

    /// Destructor
    virtual ~SshSocketHandler();

    /// Socket event handler function.
    /// Asynchronously handles ssh connection establishment and command execution.
    /// If an error is encountered during either connection establishment or
    /// command execution, then an error message is stored in #m_error and
    /// #m_cond is signalled.  Upon completion of command execution, the exit status
    /// is stored in #m_command_exit_status.
    /// @param sd Socket descriptor
    /// @param events Bitmask of polling events
    /// @see PollEvent::Flags
    /// @return <i>true</i> on success, <i>false</i> if an error was encountered.
    virtual bool handle(int sd, int events);

    /// Deregisters socket.
    /// Closes #m_sd
    /// @param sd Socket descriptor to deregister
    virtual void deregister(int sd);

    /// Returns current polling interest.
    /// @return Current polling interest
    virtual int poll_interest(int sd) { return m_poll_interest; }

    /// Writes log messages to logging output collector.
    /// If <code>priority</code> is greater than 1, this function writes
    /// <code>message</code> to the logging output buffer #m_log_buffer.  If
    /// #m_log_buffer fills up, it gets added to #m_log_collector and a new one
    /// is allocted.
    /// @param session libssh session object
    /// @param priority Message priority
    /// @param message Log message
    void log_callback(ssh_session session, int priority, const char *message);

    /// libssh authorization callback.
    /// Currently this function just logs a message and returns -1
    /// @param prompt Prompt to be displayed.                                                                                                                 
    /// @param buf %Buffer to save the password.
    /// @param len Length of the buffer.                                                                                                                   
    /// @param echo Enable or disable the echo of what you type.                                                                                            
    /// @param verify Should the password be verified?       
    /// @return -1
    int auth_callback(const char *prompt, char *buf, size_t len,
                      int echo, int verify);

    /// libssh connection status callback.
    /// This method currently does nothing.
    /// @param status Percentage of connection status, going from 0.0 to 1.0
    void connect_status_callback(float status);

    /// libssh global request callback.
    /// This function currenly just logs a message and returns.
    /// @param session libssh session object
    /// @param message libssh message
    void global_request_callback(ssh_session session, ssh_message message);

    /// libssh exit status callback
    /// This function sets #m_command_exit_status to <code>exit_status</code>,
    /// sets #m_command_exit_status_is_set to <i>true</i>, and then signals
    /// #m_cond.
    /// @param exit_status Exit status of issued command
    void set_exit_status(int exit_status);

    /// Waits for connection establishment.
    /// Blocks on #m_cond until in connected state or #m_error is non-empty or
    /// #m_cancelled is <i>true</i>.  If connection is not established by
    /// <code>deadline</code> then #m_error is set to "timeout" and <i>false</i>
    /// is returned.
    /// @param deadline Maximum wait time
    /// @return <i>true</i> upon successful connection, <i>false</i> if
    /// connection attempt failed (#m_error is non-empty) or
    /// <code>deadline</code> was reached before connection could be established
    bool wait_for_connection(std::chrono::system_clock::time_point deadline);

    /// Asynchronously issues a command.
    /// If an error is encountered while attempting to issue command and error
    /// message is stored in #m_error and <i>false</i> is returned.
    /// @param command Command to issue
    /// @return <i>true</i> if command was successfully issued, <i>false</i>
    /// otherwise.
    bool issue_command(const std::string &command);

    /// Waits for command completion.
    /// This function blocks on #m_cond until the command previously issued by
    /// issue_command() has completed or stopped due to an error or #m_cancelled
    /// was set to <i>true</i>.
    /// @return <i>true</i> if command successfully completed, <i>false</i> if
    /// #m_error is contains an error message or #m_command_exit_status was set
    /// to a non-zero value or #m_cancelled is <i>true</i> and #m_state is not
    /// STATE_CONNECTED.
    bool wait_for_command_completion();

    /// Cancels outstanding connection establishment or command execution.
    /// Sets #m_cancelled to <i>true</i>, closes #m_channel if it has been
    /// opened, closes #m_ssh_session if it is open, and then signals #m_cond.
    void cancel();

    /// Writes collected log messages to output stream.
    /// This function adds #m_log_buffer to #m_log_collector and then iterates
    /// over #m_log_collector writing each line to <code>out</code>.  If
    /// #m_error contains an error message, it is written to <code>out</code> as
    /// well.
    /// @param out Output stream to write log messages to.
    void dump_log(std::ostream &out);

    /// Tells handler to send collected output subsequent output to terminal
    /// If <code>val</code> is <i>true</i>, sends any collected stdout or stderr
    /// output to terminal and sets #m_terminal_output to <i>true</i> which causes any
    /// subsequent output collected to be sent to the terminal.  Otherwise,
    /// #m_terminal_output is set to <i>false</i> which prevents subsequent
    /// output from being sent to the terminal.
    /// @param val If <i>true</i>, enable terminal output, otherwise disable it
    void set_terminal_output(bool val);

    /// Enables debug logging output.
    /// Sets #ms_debug_enabled to <i>true</i> to cause verbose logging messages
    /// to be displayed to stdout.
    static void enable_debug();

    /// Sets libssh logging verbosity level.
    /// Parses <code>value</code> and sets #ms_libssh_verbosity accordingly.
    /// The valid values for <code>value</code> are: none, warning, protocol,
    /// packet, functions.  If <code>value</code> does not match
    /// (case insensitively) any of the value values, then an error message is
    /// written to the console and exit is called with status code 1.
    static void set_libssh_verbosity(const std::string &value);

    /// Returns hostname
    /// @return Hostname
    const std::string hostname() const { return m_hostname; }

  private:

    /// Verifies host with public key method.
    /// Verifies host and writes known host file.  #m_error is populated with an
    /// error message on failure.
    /// @return <i>true</i> on success, <i>false</i> on error.
    bool verify_knownhost();

    /// Writes output to stdout
    /// Writes output to stdout, prefixing each line with '[' hostname ']'
    /// @param output Pointer to output data
    /// @param len Length of data pointed to by <code>output</code>
    void write_to_stdout(const char *output, size_t len);

    /// Writes output to stderr
    /// Writes output to stderr, prefixing each line with '[' hostname ']'
    /// @param output Pointer to output data
    /// @param len Length of data pointed to by <code>output</code>
    void write_to_stderr(const char *output, size_t len);

    /// Determines if data available on socket for reading
    /// Checks socket descriptor #m_sd to see if there is any data available for
    /// reading.
    /// @return <i>true</i> if data is available, <i>false</i> otherwise
    bool socket_has_data();

    /// Flag for enabling debugging output.
    static bool ms_debug_enabled;

    /// Libssh logging verbosity level
    static int ms_libssh_verbosity;

    /// %Mutex for serialzing access to members
    std::mutex m_mutex;

    /// Condition variable signalling connection and command completion
    std::condition_variable m_cond;

    /// Pointer to comm layer
    Comm *m_comm;

    /// Name of host to connect to
    std::string m_hostname;

    /// libssh sesison object
    ssh_session m_ssh_session;

    /// Address of connection
    CommAddress m_comm_address;

    /// libssh channel object
    ssh_channel m_channel;

    /// Current handler state
    int m_state {};

    /// Socket descriptor
    int m_sd {};

    /// Current polling interest
    int m_poll_interest {};

    /// %Error message
    std::string m_error;

    /// libssh callbacks
    ssh_callbacks_struct m_callbacks;

    /// libssh channel callbacks
    ssh_channel_callbacks_struct m_channel_callbacks;

    /// Current command being issued
    std::string m_command;

    /// Command exit status
    int m_command_exit_status {};

    /// Flag indicating that the exit status has been set
    bool m_command_exit_status_is_set {};

    /// Redirect output to terminal
    bool m_terminal_output {};

    /// Line prefix needs to be emitted on next stdout output
    bool m_line_prefix_needed_stdout {};

    /// Line prefix needs to be emitted on next stderr output
    bool m_line_prefix_needed_stderr {};

    /// Flag indicating that outstanding operations should be cancelled
    bool m_cancelled {};

    /// Flag indicating that current channel is EOF
    bool m_channel_is_eof {};

    /// Output collector for logging
    SshOutputCollector m_log_collector;

    /// Current logging output buffer
    SshOutputCollector::Buffer m_log_buffer {};

    /// Output collector for stdout
    SshOutputCollector m_stdout_collector;

    /// Current stdout output buffer
    SshOutputCollector::Buffer m_stdout_buffer {};

    /// Output collector for stderr
    SshOutputCollector m_stderr_collector;

    /// Current stderr output buffer
    SshOutputCollector::Buffer m_stderr_buffer {};
    
  };

  /// Smart pointer to SshSocketHandler
  typedef std::shared_ptr<SshSocketHandler> SshSocketHandlerPtr;

  /// @}

} // namespace Hypertable

#endif // Tools_cluster_SshSocketHandler_h
