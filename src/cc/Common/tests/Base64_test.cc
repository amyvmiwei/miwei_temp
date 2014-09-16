/*
 * Copyright (C) 2007-2014 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3
 * of the License.
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

#include <Common/Compat.h>
#include <Common/Base64.h>
#include <Common/Logger.h>

#include <iostream>

using namespace Hypertable;
using namespace std;

namespace {

  const char *message_str =
    "Well, Prince, so Genoa and Lucca are now just family estates of the "
    "Buonapartes. But I warn you, if you don't tell me that this means war, "
    "if you still try to defend the infamies and horrors perpetrated by that "
    "Antichrist--I really believe he is Antichrist--I will have nothing more "
    "to do with you and you are no longer my friend, no longer my 'faithful "
    "slave,' as you call yourself! But how do you do? I see I have frightened "
    "you--sit down and tell me all the news.";

}

int main(int argc, char **argv) {

  string original_message(message_str);
  string encoded_message = Base64::encode(original_message);
  string decoded_message = Base64::decode(encoded_message);

  cout << "\n[Message encoded]" << endl;
  cout << encoded_message << endl;
  cout << "\n[Message decoded]" << endl;
  cout << decoded_message << endl;

  HT_ASSERT(original_message.compare(decoded_message) == 0);

  encoded_message = Base64::encode(original_message, true);
  decoded_message = Base64::decode(encoded_message, true);

  cout << "\n[Message encoded with newlines]" << endl;
  cout << encoded_message << endl;
  cout << "\n[Message decoded with newlines]" << endl;
  cout << decoded_message << endl;

  HT_ASSERT(original_message.compare(decoded_message) == 0);

  return 0;
}
