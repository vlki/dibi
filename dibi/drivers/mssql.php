<?php

/**
 * dibi - tiny'n'smart database abstraction layer
 * ----------------------------------------------
 *
 * Copyright (c) 2005, 2009 David Grudl (http://davidgrudl.com)
 *
 * This source file is subject to the "dibi license" that is bundled
 * with this package in the file license.txt.
 *
 * For more information please see http://dibiphp.com
 *
 * @copyright  Copyright (c) 2005, 2009 David Grudl
 * @license    http://dibiphp.com/license  dibi license
 * @link       http://dibiphp.com
 * @package    dibi
 */


/**
 * The dibi driver for MS SQL database.
 *
 * Connection options:
 *   - 'host' - the MS SQL server host name. It can also include a port number (hostname:port)
 *   - 'username' (or 'user')
 *   - 'password' (or 'pass')
 *   - 'persistent' - try to find a persistent link?
 *   - 'database' - the database name to select
 *   - 'lazy' - if TRUE, connection will be established only when required
 *   - 'resource' - connection resource (optional)
 *   - 'dbcharset' - character encoding of database (optional)
 *   - 'charset' - character encoding of input and output (optional)
 *
 * @author     David Grudl
 * @copyright  Copyright (c) 2005, 2009 David Grudl
 * @package    dibi
 */
class DibiMsSqlDriver extends DibiObject implements IDibiDriver
{
	/** @var resource  Connection resource */
	private $connection;

	/** @var resource  Resultset resource */
	private $resultSet;

	/** @var string  Databases character encoding */
	private $dbcharset = 'Windows-1250';

	/** @var string  Input and output character encoding */
        private $charset = 'UTF-8';

	/** @var mixed  SQL part for order by clause */
	public $orderBy;


	/**
	 * @throws DibiException
	 */
	public function __construct()
	{
		if (!extension_loaded('mssql')) {
			throw new DibiDriverException("PHP extension 'mssql' is not loaded.");
		}
	}



	/**
	 * Connects to a database.
	 * @return void
	 * @throws DibiException
	 */
	public function connect(array &$config)
	{
		if (isset($config['resource'])) {
			$this->connection = $config['resource'];
		} elseif (empty($config['persistent'])) {
			$this->connection = @mssql_connect($config['host'], $config['username'], $config['password'], TRUE); // intentionally @
		} else {
			$this->connection = @mssql_pconnect($config['host'], $config['username'], $config['password']); // intentionally @
		}

		if (!is_resource($this->connection)) {
			throw new DibiDriverException("Can't connect to DB.");
		}

		if (isset($config['database']) && !@mssql_select_db($config['database'], $this->connection)) { // intentionally @
			throw new DibiDriverException("Can't select DB '$config[database]'.");
		}

		if (isset($config['dbcharset'])) $this->dbcharset = $config['dbcharset'];
		if (isset($config['charset'])) $this->charset = $config['charset'];
	}



	/**
	 * Disconnects from a database.
	 * @return void
	 */
	public function disconnect()
	{
		mssql_close($this->connection);
	}



	/**
	 * Executes the SQL query.
	 * @param  string      SQL statement.
	 * @return IDibiDriver|NULL
	 * @throws DibiDriverException
	 */
	public function query($sql)
	{
		if ($this->dbcharset !== NULL && $this->charset !== NULL) {
			$sql = iconv($this->charset, $this->dbcharset . '//IGNORE', $sql);
		}

		$this->resultSet = @mssql_query($sql, $this->connection); // intentionally @

		if ($this->resultSet === FALSE) {
			throw new DibiDriverException('Query error', 0, $sql);
		}

		return is_resource($this->resultSet) ? clone $this : NULL;
	}



	/**
	 * Gets the number of affected rows by the last INSERT, UPDATE or DELETE query.
	 * @return int|FALSE  number of rows or FALSE on error
	 */
	public function getAffectedRows()
	{
		return mssql_rows_affected($this->connection);
	}



	/**
	 * Retrieves the ID generated for an AUTO_INCREMENT column by the previous INSERT query.
	 * @return int|FALSE  int on success or FALSE on failure
	 */
	public function getInsertId($sequence)
	{
		$res = mssql_query('SELECT @@IDENTITY', $this->connection);
		if (is_resource($res)) {
			$row = mssql_fetch_row($res);
			return $row[0];
		}
		return FALSE;
	}



	/**
	 * Begins a transaction (if supported).
	 * @param  string  optional savepoint name
	 * @return void
	 * @throws DibiDriverException
	 */
	public function begin($savepoint = NULL)
	{
		$this->query('BEGIN TRANSACTION');
	}



	/**
	 * Commits statements in a transaction.
	 * @param  string  optional savepoint name
	 * @return void
	 * @throws DibiDriverException
	 */
	public function commit($savepoint = NULL)
	{
		$this->query('COMMIT');
	}



	/**
	 * Rollback changes in a transaction.
	 * @param  string  optional savepoint name
	 * @return void
	 * @throws DibiDriverException
	 */
	public function rollback($savepoint = NULL)
	{
		$this->query('ROLLBACK');
	}



	/**
	 * Returns the connection resource.
	 * @return mixed
	 */
	public function getResource()
	{
		return $this->connection;
	}



	/********************* SQL ****************d*g**/



	/**
	 * Encodes data for use in a SQL statement.
	 * @param  mixed     value
	 * @param  string    type (dibi::TEXT, dibi::BOOL, ...)
	 * @return string    encoded value
	 * @throws InvalidArgumentException
	 */
	public function escape($value, $type)
	{
		switch ($type) {
		case dibi::TEXT:
		case dibi::BINARY:
			return "'" . str_replace("'", "''", $value) . "'";

		case dibi::IDENTIFIER:
			// @see http://msdn.microsoft.com/en-us/library/ms176027.aspx
			$value = str_replace(array('[', ']'), array('[[', ']]'), $value);
			return '[' . str_replace('.', '].[', $value) . ']';

		case dibi::BOOL:
			return $value ? 1 : 0;

		case dibi::DATE:
			return $value instanceof DateTime ? $value->format("'Y-m-d'") : date("'Y-m-d'", $value);

		case dibi::DATETIME:
			return $value instanceof DateTime ? $value->format("'Y-m-d H:i:s'") : date("'Y-m-d H:i:s'", $value);

		default:
			throw new InvalidArgumentException('Unsupported type.');
		}
	}



	/**
	 * Decodes data from result set.
	 * @param  string    value
	 * @param  string    type (dibi::BINARY)
	 * @return string    decoded value
	 * @throws InvalidArgumentException
	 */
	public function unescape($value, $type)
	{
		if ($type === dibi::BINARY) {
			return $value;
		}
		throw new InvalidArgumentException('Unsupported type.');
	}



	/**
	 * Injects LIMIT/OFFSET to the SQL query.
	 * @param  string &$sql  The SQL query that will be modified.
	 * @param  int $limit
	 * @param  int $offset
	 * @return void
	 */
	public function applyLimit(&$sql, $limit, $offset)
	{
		$orderBy = '';
		if (isset($this->orderBy)) {
			$orderBy = $this->orderBy;
			$this->orderBy = NULL;

			// from DibiTranslator
			$vx = $vtx = array();
			foreach ($orderBy as $k => $v) {
				if (is_string($k)) {
					$v = (is_string($v) && strncasecmp($v, 'd', 1)) || $v > 0 ? 'ASC' : 'DESC';
					$vt = (is_string($v) && strncasecmp($v, 'd', 1)) || $v > 0 ? 'DESC' : 'ASC';
					$vx[] = $this->escape($k, dibi::IDENTIFIER) . ' ' . $v;
					$vtx[] = $this->escape($k, dibi::IDENTIFIER) . ' ' . $vt;
				} else {
					$vx[] = $this->escape($v, dibi::IDENTIFIER);
					$vtx[] = $this->escape($v, dibi::IDENTIFIER);
				}
			}
			$orderBy = implode(', ', $vx);
			$orderByTwisted = implode(', ', $vtx);
		}
		
		// @see http://josephlindsay.com/archives/2005/05/27/paging-results-in-ms-sql-server/
		if ($limit >= 0 && $offset && $orderBy !== '') {
			$sql = 'SELECT TOP ' . (int) $limit . ' * FROM (
					SELECT TOP ' . (int) $limit . ' * FROM (
						SELECT TOP ' . ((int) $limit + (int) $offset)  . ' * FROM (
							' . $sql . '
						) t ' . ($orderBy !== '' ? 'ORDER BY ' . $orderBy : '') . '
					) t ' . ($orderBy !== '' ? 'ORDER BY ' . $orderByTwisted : '') . '
				) t ' . ($orderBy !== '' ? 'ORDER BY ' . $orderBy : '');

		} elseif ($limit >= 0) {
			$sql = 'SELECT TOP ' . (int) $limit . ' * FROM (' . $sql . ') t ' . ($orderBy !== '' ? 'ORDER BY ' . $orderBy : '');

		} else {
			throw new NotImplementedException('Offset without limit or sorting is not implemented.');
		}
	}



	/********************* result set ****************d*g**/



	/**
	 * Returns the number of rows in a result set.
	 * @return int
	 */
	public function getRowCount()
	{
		return mssql_num_rows($this->resultSet);
	}



	/**
	 * Fetches the row at current position and moves the internal cursor to the next position.
	 * @param  bool     TRUE for associative array, FALSE for numeric
	 * @return array    array on success, nonarray if no next record
	 * @internal
	 */
	public function fetch($assoc)
	{
		$row = mssql_fetch_array($this->resultSet, $assoc ? MSSQL_ASSOC : MSSQL_NUM);

		if (is_array($row)) {
			foreach($row as &$v) {
				if (is_string($v) && $this->dbcharset !== NULL && $this->charset !== NULL) {
					$v = iconv($this->dbcharset, $this->charset . '//IGNORE', $v);
				}
			}
		}

		return $row;
	}



	/**
	 * Moves cursor position without fetching row.
	 * @param  int      the 0-based cursor pos to seek to
	 * @return boolean  TRUE on success, FALSE if unable to seek to specified record
	 */
	public function seek($row)
	{
		return mssql_data_seek($this->resultSet, $row);
	}



	/**
	 * Frees the resources allocated for this result set.
	 * @return void
	 */
	public function free()
	{
		mssql_free_result($this->resultSet);
		$this->resultSet = NULL;
	}



	/**
	 * Returns metadata for all columns in a result set.
	 * @return array
	 */
	public function getColumnsMeta()
	{
		$count = mssql_num_fields($this->resultSet);
		$res = array();
		for ($i = 0; $i < $count; $i++) {
			$row = (array) mssql_fetch_field($this->resultSet, $i);
			$res[] = array(
				'name' => $row['name'],
				'fullname' => $row['column_source'] ? $row['column_source'] . '.' . $row['name'] : $row['name'],
				'table' => $row['column_source'],
				'nativetype' => $row['type'],
			);
		}
		return $res;
	}



	/**
	 * Returns the result set resource.
	 * @return mixed
	 */
	public function getResultResource()
	{
		return $this->resultSet;
	}



	/********************* reflection ****************d*g**/



	/**
	 * Returns list of tables.
	 * @return array
	 */
	public function getTables()
	{
		throw new NotImplementedException;
	}



	/**
	 * Returns metadata for all columns in a table.
	 * @param  string
	 * @return array
	 */
	public function getColumns($table)
	{
		throw new NotImplementedException;
	}



	/**
	 * Returns metadata for all indexes in a table.
	 * @param  string
	 * @return array
	 */
	public function getIndexes($table)
	{
		throw new NotImplementedException;
	}



	/**
	 * Returns metadata for all foreign keys in a table.
	 * @param  string
	 * @return array
	 */
	public function getForeignKeys($table)
	{
		throw new NotImplementedException;
	}

}
