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


function showTweets($list)
{
  if (!$list)
    return;
  echo '<table>';
  /* show timestamp, avatar, username (both with link to profile) */
  foreach ($list as $tweet) {
    echo '<tr><td style="padding-bottom: 1em;">';
    $profile=ProfileTable::load($tweet->getAuthor());
    if ($profile and $profile->getAvatar())
      echo '<a href="/profile/show/'.$tweet->getAuthor().'">'.
        '<img src="'.$profile->getAvatar().'" width="64" height="64" /></a>'.
        '<br />';
    echo '<a href="/profile/show/'.$tweet->getAuthor().'">'.
        $tweet->getAuthor().'</a>';
    echo '</td><td style="padding-bottom: 1em;">';
    echo $tweet->getMessage();
    echo "<br />";
    echo '<span style="font-size: 80%;">'.
        date('r', $tweet->getTimestamp()/1000000000).
        '</span>';
    echo '</td></tr>';
  }
  echo '</table>';
}

function showUsers($list)
{
  echo '<table>';
  /* show avatar, username, location, bio (with link to profile) */
  foreach ($list as $user) {
    echo '<tr><td style="padding-bottom: 1em;">';
    $profile=ProfileTable::load($user);
    if ($profile->getAvatar())
      echo '<a href="/profile/show/'.$user.'">'.
        '<img src="'.$profile->getAvatar().'" width="64" height="64" /></a>'.
        '<br />';
    echo '<a href="/profile/show/'.$user.'">'.$user.'</a>';
    echo '</td><td style="padding-bottom: 1em;">';
    echo $profile->getBio();
    echo "<br />";
    echo '<span style="font-size: 80%;">'.$profile->getLocation().'</span>';
    echo '</td></tr>';
  }
  echo '</table>';
}

?>
