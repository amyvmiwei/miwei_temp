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


/**
 * This class caches our Hypertable handles and offers helper functions
 * to query, insert or delete keys
 */
class HypertableConnection
{
  // Sends a query to the Hypertable server
  public static function query($hql) {
    self::initialize();
    return self::$_client->hql_query(self::$_namespace, $hql);
  }

  // Inserts a cell into the Database
  public static function insert($table, $row, $column, $value, $timestamp=0) {
    self::initialize();
    if (!$timestamp)
      $hql="INSERT INTO $table VALUES ('$row', '$column', '$value')";
    else
      $hql="INSERT INTO $table VALUES ('$timestamp', '$row', '$column', ".
                                     "'$value')";
    return self::$_client->hql_query(self::$_namespace, $hql);
  }

  // Deletes a cell from the Database
  public static function delete($table, $row, $column, $timestamp=0) {
    self::initialize();
    if ($timestamp==0)
      $hql="DELETE \"$column\" FROM $table WHERE ROW = \"$row\"";
    else
      $hql="DELETE \"$column\" FROM $table WHERE ROW = \"$row\" ".
           "VERSION $timestamp";
    return self::$_client->hql_query(self::$_namespace, $hql);
  }

  // hypertable timestamps are stored in nanoseconds, but HQL needs 
  // a timestamp like '2001-10-20 12:34:01:23984'
  public static function format_timestamp_ns($ts) {
    $epoch=(int)($ts/1000000000);
    $rem=$ts%1000000000;
    date_default_timezone_set('UTC');
    return sprintf("%s:%u", date("Y-m-d H:i:s", $epoch), $rem);
  }

  // create and insert a unique value
  public static function create_cell_unique($table, $row, $cf) {
    self::initialize();

    $k=new Key();
    $k->row=$row;
    $k->column_family=$cf;
    return self::$_client->create_cell_unique(self::$_namespace, 
                $table, $k, '');
  }

  // Close the namespace to avoid leaks in the ThriftBroker
  public static function close() {
    if (self::$_namespace) {
      self::$_client->close_namespace(self::$_namespace);
      self::$_namespace=null;
    }
  }

  // Creates a new connection and returns the Namespace object.
  // The connection and the namespace are cached.
  protected static function initialize() {
    $bootstrap = Zend_Controller_Front::getInstance()->getParam('bootstrap');
    $options = $bootstrap->getOptions();
    if (!self::$_client) {
      $host=$options['hypertable']['thriftclient']['hostname']; 
      $port=$options['hypertable']['thriftclient']['port']; 
      self::$_client=new Hypertable_ThriftClient($host, $port);
    }
    if (!self::$_namespace) {
      $ns=$options['hypertable']['namespace']; 
      self::$_namespace=self::$_client->open_namespace($ns);
    }
  }

  protected static $_client;         // Hypertable database connection
  protected static $_namespace;      // Hypertable Namespace object
}

?>
