<?
/**
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


require_once 'HypertableConnection.php';
require_once 'Tweet.php';
require_once 'UserTable.php';
require_once 'User.php';

class TweetTable
{
  // load a tweet from the database
  public function load($id) {
    $result=HypertableConnection::query("SELECT * FROM tweet ".
                "WHERE ROW='$id'");
    if (!$result or !count($result->cells))
      return null;

    $tweet=new Tweet();
    $tweet->setId($id);
    $tweet->setTimestamp($result->cells[0]->key->timestamp);
    $tweet->setMessage($result->cells[0]->value);
    return $tweet;
  }

  // Store a tweet; this function will automatically assign a new ID
  // if necessary 
  public function store($tweet, $author) {
    if (!$tweet->getId())
      $tweet->createId($author);
    HypertableConnection::insert('tweet', $tweet->getId(), 'message',
            $tweet->getMessage());
    $u=UserTable::load($author);
    $u->sendTweet($tweet);
    return true;
  }
}

?>
