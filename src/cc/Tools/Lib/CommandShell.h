/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

#ifndef HYPERTABLE_COMMAND_SHELL_H
#define HYPERTABLE_COMMAND_SHELL_H

#include <histedit.h>

#include "Common/ReferenceCount.h"
#include "Common/Properties.h"

#include "CommandInterpreter.h"
#include "Notifier.h"

namespace Hypertable {

  class CommandShell : public ReferenceCount {
  public:
    CommandShell(const String &program_name, CommandInterpreterPtr &,
                 PropertiesPtr &);

    ~CommandShell();

    int run();

    bool silent() { return m_silent; }
    bool test_mode() { return m_test_mode; }
    void set_namespace(const String &ns) { m_namespace=ns; }

    static void add_options(PropertiesDesc &);

    static String ms_history_file;

  private:
    char *rl_gets();
    static CommandShell *ms_instance;
    static const wchar_t *prompt(EditLine *el);

    String m_program_name;
    CommandInterpreterPtr m_interp_ptr;
    PropertiesPtr m_props;
    NotifierPtr m_notifier_ptr;

    String m_accum;
    bool m_batch_mode;
    bool m_silent;
    bool m_test_mode;
    bool m_no_prompt;
    bool m_cont;
    char *m_line_read;
    bool m_notify;
    bool m_stdin;
    bool m_has_cmd_file;
    bool m_has_cmd_exec;
    String m_input_str;
    std::wstring m_prompt_str;
    String m_cmd_str;
    String m_cmd_file;
    String m_namespace;

    EditLine *m_editline;
    HistoryW *m_history;
    HistEventW m_history_event;
    TokenizerW *m_tokenizer;
  };

  typedef intrusive_ptr<CommandShell> CommandShellPtr;

} // namespace Hypertable

#endif // HYPERTABLE_COMMAND_SHELL_H

