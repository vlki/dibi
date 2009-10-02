<?php

/**
 * Mssql implementation of IDataSource for dibi.
 *
 * @author     Jan Vlcek
 * @copyright  Copyright (c) 2009 Jan Vlcek
 * @package    dibi
 */
class DibiMssqlDataSource extends DibiDataSource
{

	/** @var mixed  Default sorting. Because mssql need some order by when paging results. */
	protected $defaultSorting;


	/**
	 * Returns SQL query.
	 * @return string
	 * @throws DibiException
	 */
	public function __toString()
	{
		if (!isset($this->defaultSorting)) {
			throw new DibiException('Default sorting for DibiMssqlDataSource must be set.');
		}

		$this->connection->driver->orderBy = $this->sorting ? $this->sorting + $this->defaultSorting : $this->defaultSorting;
		return $this->connection->sql('
			SELECT %n', (empty($this->cols) ? '*' : $this->cols), '
			FROM %SQL', $this->sql, '
			%ex', $this->conds ? array('WHERE %and', $this->conds) : NULL, '
			%ofs %lmt', $this->offset, $this->limit
		);
	}

	/**
	 * Returns default sorting for this data source
	 * @return mixed
	 */
	public function getDefaultSorting()
	{
		return $this->defaultSorting;
	}

	/**
	 * Sets default sorting for this data source. Format could be used
	 * the same as in DibiFluent orderBy command.
	 * @return mixed
	 */
	public function setDefaultSorting($sorting)
	{
		$this->defaultSorting = $sorting;
		return $this;
	}

	/**
	 * Could not be wrapped into other data source because of default sorting.
	 * @throws DibiException
	 */
	public function toDataSource()
	{
		throw new DibiException($this->getClass() . ' could not be wrapped in other data source.');
	}

}
