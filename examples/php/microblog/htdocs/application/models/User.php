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
require_once 'TweetTable.php';

class User
{
  public function getId() {
    return ($this->_id);
  }

  public function setId($id) {
    $this->_id=$id;
  }

  public function follow($other) {
    $id=$this->_id;
    HypertableConnection::insert('user', $id, "following:$other",
            '');
    HypertableConnection::insert('user', $id, 'following_count',
            1);
    HypertableConnection::insert('user', $other, 'followers:'.$id, 
            '');
    HypertableConnection::insert('user', $other, 'followers_count',
            1);

    // obtain the timestamps
    $result=HypertableConnection::query("SELECT CELLS * FROM user WHERE ".
            "(CELL = '$id','following:$other' ".
            "OR CELL = '$other','followers:$id')");
    $following_timestamp=$result->cells[0]->key->timestamp;
    $followers_timestamp=$result->cells[1]->key->timestamp;

    // insert into the histories with the timestamps
    HypertableConnection::insert('user', $id, 'following_history', $other,
            HypertableConnection::format_timestamp_ns($following_timestamp));
    HypertableConnection::insert('user', $other, 'followers_history', $id,
            HypertableConnection::format_timestamp_ns($followers_timestamp));
  }

  public function unfollow($other) {
    // obtain the timestamps
    $result=HypertableConnection::query("SELECT CELLS * FROM user WHERE ".
            "(CELL = '$id','following:$other' ".
            "OR CELL = '$other','followers:$id')");
    $following_timestamp=$result->cells[0]->key->timestamp;
    $followers_timestamp=$result->cells[1]->key->timestamp;

    // DELETE following_history and followers_history
    HypertableConnection::delete('user', $this->_id, 'following_history',
            $following_timestamp);
    HypertableConnection::delete('user', $other, 'followers_history',
            $followers_timestamp);

    // DELETE following and followers
    HypertableConnection::delete('user', $this->_id, "following:$other");
    HypertableConnection::delete('user', $other, 'followers:'.$this->_id);

    // decrement the counters
    HypertableConnection::insert('user', $this->_id, 'following_count', '-1');
    HypertableConnection::insert('user', $other, 'followers_count', '-1');
  }

  public function getFollowingCount() {
    $result=HypertableConnection::query("SELECT following_count FROM ".
                "user WHERE ROW='".$this->_id."'");
    if (!$result || count($result->cells)==0)
        return 0;
    return $result->cells[0]->value;
  }

  public function getFollowing($cutoff_time, $limit) {
    $a=array();
    $id=$this->_id;
    if ($cutoff_time)
      $t="AND TIMESTAMP < '".
         HypertableConnection::format_timestamp_ns($cutoff_time)."' ";
    else
      $t='';
    $result=HypertableConnection::query("SELECT following_history FROM user ".
                "WHERE ROW='$id' $t CELL_LIMIT $limit");
    if (!$result or !count($result->cells))
      return $a;
    foreach ($result->cells as $cell) {
      array_push($a, $cell->value);
    }
    $this->_cutoff=$result->cells[count($result->cells)-1]->key->timestamp;
    return $a;
  }

  public function isFollowing($other) {
    $id=$this->_id;
    $result=HypertableConnection::query("SELECT CELLS following:$other FROM ".
                "user WHERE ROW='$id'");
    if (!$result)
        return null;
    return count($result->cells);
  }

  public function getFollowersCount() {
    $result=HypertableConnection::query("SELECT followers_count FROM ".
                "user WHERE ROW='".$this->_id."'");
    if (!$result || count($result->cells)==0)
        return 0;
    return $result->cells[0]->value;
  }

  public function getFollowers($cutoff_time, $limit) {
    $a=array();
    $id=$this->_id;
    if ($cutoff_time)
      $t="AND TIMESTAMP < '".
         HypertableConnection::format_timestamp_ns($cutoff_time)."' ";
    else
      $t='';
    $result=HypertableConnection::query("SELECT followers_history FROM user ".
                "WHERE ROW='$id' $t CELL_LIMIT $limit");
    if (!$result or !count($result->cells))
      return $a;
    foreach ($result->cells as $cell) {
      array_push($a, $cell->value);
    }
    $this->_cutoff=$result->cells[count($result->cells)-1]->key->timestamp;
    return $a;
  }

  public function sendTweet($tweet) {
    $tid=$tweet->getId();
    HypertableConnection::insert('user', $this->_id, "my_stream",
            $tid);
    HypertableConnection::insert('user', $this->_id, 'my_stream_count',
            1);

    // store in follow_stream, otherwise the user's own tweets would not
    // be displayed in the timeline
    HypertableConnection::insert('user', $this->_id, 'follow_stream',
            $tid);

    // get followers
    $result=HypertableConnection::query("SELECT followers FROM ".
            "user WHERE ROW='".$this->_id."'");

    // foreach follower: store tweet id in their "follow-stream"
    foreach ($result->cells as $cell) {
      $qualifier=$cell->key->column_qualifier;
      HypertableConnection::insert('user', $qualifier, 'follow_stream',
            $tid);
      HypertableConnection::insert('user', $qualifier, 'follow_stream_count',
            '+1');
    }
  }

  public function getMyStreamCount() {
    $result=HypertableConnection::query("SELECT my_stream_count FROM ".
                "user WHERE ROW='".$this->_id."'");
    if (!$result || !count($result->cells))
      return 0;
    return $result->cells[0]->value;
  }

  public function getMyStream($cutoff_time, $limit) {
    $a=array();
    $id=$this->_id;
    if ($cutoff_time)
      $t="AND TIMESTAMP < '".
         HypertableConnection::format_timestamp_ns($cutoff_time)."' ";
    else
      $t='';
    $result=HypertableConnection::query("SELECT my_stream FROM user ".
                "WHERE ROW='$id' $t CELL_LIMIT $limit");
    if (!$result or !count($result->cells))
      return $a;
    foreach ($result->cells as $cell) {
      array_push($a, TweetTable::load($cell->value));
    }
    $this->_cutoff=$result->cells[count($result->cells)-1]->key->timestamp;
    return $a;
  }

  public function getFollowStreamCount() {
    $result=HypertableConnection::query("SELECT follow_stream_count FROM ".
                "user WHERE ROW='".$this->_id."'");
    if (!$result || !count($result->cells))
      return 0;
    return $result->cells[0]->value;
  }

  public function getFollowStream($cutoff_time, $limit) {
    $a=array();
    $id=$this->_id;
    if ($cutoff_time)
      $t="AND TIMESTAMP < '".
         HypertableConnection::format_timestamp_ns($cutoff_time)."' ";
    else
      $t='';
    $result=HypertableConnection::query("SELECT follow_stream FROM user ".
                "WHERE ROW='$id' $t CELL_LIMIT $limit");
    if (!$result or !count($result->cells))
      return $a;
    foreach ($result->cells as $cell) {
      array_push($a, TweetTable::load($cell->value));
    }
    $this->_cutoff=$result->cells[count($result->cells)-1]->key->timestamp;
    return $a;
  }

  public function getCutoffTime() {
    return $this->_cutoff;
  }

  protected $_id;
  protected $_cutoff;
}

?>
