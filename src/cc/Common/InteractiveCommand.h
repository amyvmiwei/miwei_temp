/*
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

/** @file
 * Base class for interactive shell commands.
 */

#ifndef HYPERTABLE_INTERACTIVECOMMAND_H
#define HYPERTABLE_INTERACTIVECOMMAND_H

#include <cstring>

#include <utility>
#include <vector>

namespace Hypertable {

/** @addtogroup Common
 *  @{
 */

/**
 * Abstract base class for simple interactive shell commands. The base
 * class parses the command and runs it. For a usage example see the
 * FsBroker client shell: src/cc/Tools/fsclient/fsclient.cc.
 */
class InteractiveCommand {
  public:
    /** Virtual destructor can be overwritten in subclasses */
    virtual ~InteractiveCommand() { }

    /** Parses a command line and stores the arguments in m_args. This
     * function will exit() if the command line cannot be parsed.
     *
     * @param line A command line; can contain quoted strings
     */
    void parse_command_line(const char *line);

    /** Returns true if the command_text() matches the command line
     */
    bool matches(const char *line) {
      return !strncmp(line, command_text(), strlen(command_text()));
    }

    /** Returns the command text (i.e. "length")
     *
     * Abstract function; needs to be overwritten in a subclass!
     */
    virtual const char *command_text() = 0;

    /** Returns the usage string
     *
     * Abstract function; needs to be overwritten in a subclass!
     */
    virtual const char **usage() = 0;

    /** Runs the command; can access the command line arguments in m_args */
    virtual void run() = 0;

    /** Clears all arguments */
    void clear_args() { m_args.clear(); }

    /** Append another key/value pair to the argument list */
    void push_arg(std::string key, std::string value) {
      m_args.push_back(std::pair<std::string, std::string>(key, value));
    }

  protected:
    /** The vector with key/value pairs for this command */
    std::vector< std::pair<std::string, std::string> >  m_args;

    /** Helper function to parse a string literal */
    bool parse_string_literal(const char *str, std::string &text,
            const char **endptr);
};

/** @} */

}

#endif // HYPERTABLE_INTERACTIVECOMMAND_H
