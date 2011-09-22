<?php

// Define path to application directory
defined('APPLICATION_PATH')
    || define('APPLICATION_PATH', realpath(dirname(__FILE__) . '/../application'));

// Define application environment
defined('APPLICATION_ENV')
    || define('APPLICATION_ENV', (getenv('APPLICATION_ENV') ? getenv('APPLICATION_ENV') : 'production'));

// Ensure library/ is on include_path
set_include_path(implode(PATH_SEPARATOR, array(
    realpath(APPLICATION_PATH . '/../library'),
    realpath(APPLICATION_PATH . '/../application/models'),
    get_include_path(),
)));

// Zend_Application
require_once 'Zend/Application.php';

/**
 * Load Hypertable. The THRIFT_ROOT specifies the directory with the 
 * Hypertable Thrift client. THRIFT_ROOT can be an absolute path or relative
 * to any directory in your include path. In our case the client is in 
 * 'library/Hypertable', and 'library' is in the include path.
 * 
 * If you do not set THRIFT_ROOT then the client will follow a heuristic
 * approach to find the libraries. It is recommended to set THRIFT_ROOT.
 */
$GLOBALS['THRIFT_ROOT']='Hypertable';
require_once 'Hypertable/ThriftClient.php';

// Create application, bootstrap, and run
$application = new Zend_Application(
    APPLICATION_ENV,
    APPLICATION_PATH . '/configs/application.ini'
);
$application->bootstrap()
            ->run();

/**
 * Clean up the hypertable handles to avoid leaks in the ThriftBroker
 */
require_once 'HypertableConnection.php';
HypertableConnection::close();
