import logging
import os
import sqlite3
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any

from tabulate import tabulate


class DataBased:
    """Sqli wrapper so queries don't need to be written except table definitions.

    Supports saving and reading dates as datetime objects.

    Supports using a context manager."""

    def __init__(
        self,
        dbPath: str | Path,
        loggerEncoding: str = "utf-8",
        loggerMessageFormat: str = "{levelname}|-|{asctime}|-|{message}",
    ):
        """
        :param dbPath: String or Path object to database file.
        If a relative path is given, it will be relative to the
        current working directory. The log file will be saved to the
        same directory.

        :param loggerMessageFormat: '{' style format string
        for the logger object."""
        self.dbPath = Path(dbPath)
        self.dbName = Path(dbPath).name
        self._loggerInit(encoding=loggerEncoding, messageFormat=loggerMessageFormat)
        self.connectionOpen = False
        self.createManager()

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exceptionType, exceptionValue, exceptionTraceback):
        self.close()

    def createManager(self):
        """Create dbManager.py in the same directory
        as the database file if they don't exist."""
        managerTemplate = Path(__file__).parent / "dbManager.py"
        managerPath = self.dbPath.parent / "dbManager.py"
        if not managerPath.exists():
            managerPath.write_text(managerTemplate.read_text())

    def open(self):
        """Open connection to db."""
        self.connection = sqlite3.connect(
            self.dbPath, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
        )
        self.connection.execute("pragma foreign_keys = 1")
        self.cursor = self.connection.cursor()
        self.connectionOpen = True

    def close(self):
        """Save and close connection to db.

        Call this as soon as you are done using the database if you have
        multiple threads or processes using the same database."""
        if self.connectionOpen:
            self.connection.commit()
            self.connection.close()
            self.connectionOpen = False

    def _connect(func):
        """Decorator to open db connection if it isn't already open."""

        @wraps(func)
        def inner(*args, **kwargs):
            self = args[0]
            if not self.connectionOpen:
                self.open()
            results = func(*args, **kwargs)
            return results

        return inner

    def _loggerInit(
        self,
        messageFormat: str = "{levelname}|-|{asctime}|-|{message}",
        encoding: str = "utf-8",
    ):
        """:param messageFormat: '{' style format string"""
        self.logger = logging.getLogger(self.dbName)
        if not self.logger.hasHandlers():
            handler = logging.FileHandler(
                str(self.dbPath).replace(".", "") + ".log", encoding=encoding
            )
            handler.setFormatter(
                logging.Formatter(
                    messageFormat, style="{", datefmt="%m/%d/%Y %I:%M:%S %p"
                )
            )
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

    def _getDict(
        self, table: str, values: list, columnsToReturn: list[str] = None
    ) -> dict:
        """Converts the values of a row into a dictionary with column names as keys.

        :param table: The table that values were pulled from.

        :param values: List of values expected to be the same quantity
        and in the same order as the column names of table.

        :param columnsToReturn: An optional list of column names.
        If given, only these columns will be included in the returned dictionary.
        Otherwise all columns and values are returned."""
        return {
            column: value
            for column, value in zip(self.getColumnNames(table), values)
            if not columnsToReturn or column in columnsToReturn
        }

    def _getConditions(
        self, matchCriteria: list[tuple] | dict, exactMatch: bool = True
    ) -> str:
        """Builds and returns the conditional portion of a query.

        :param matchCriteria: Can be a list of 2-tuples where each
        tuple is (columnName, rowValue) or a dictionary where
        keys are column names and values are row values.

        :param exactMatch: If False, the rowValue for a give column
        will be matched as a substring.

        Usage e.g.:

        self.cursor.execute(f'select * from {table} where {conditions}')"""
        if type(matchCriteria) == dict:
            matchCriteria = [(k, v) for k, v in matchCriteria.items()]
        if exactMatch:
            conditions = " and ".join(
                f'"{columnRow[0]}" = "{columnRow[1]}"' for columnRow in matchCriteria
            )
        else:
            conditions = " and ".join(
                f'"{columnRow[0]}" like "%{columnRow[1]}%"'
                for columnRow in matchCriteria
            )
        return f"({conditions})"

    @_connect
    def createTables(self, tableStatements: list[str] = []):
        """Create tables if they don't exist.

        :param tableStatements: Each statement should be
        in the form 'tableName(columnDefinitions)'"""
        if len(tableStatements) > 0:
            tableNames = self.getTableNames()
            for table in tableStatements:
                if table.split("(")[0].strip() not in tableNames:
                    self.cursor.execute(f"create table {table}")
                    self.logger.info(f'{table.split("(")[0]} table created.')

    @_connect
    def getTableNames(self) -> list[str]:
        """Returns a list of table names from database."""
        self.cursor.execute(
            'select name from sqlite_Schema where type = "table" and name not like "sqlite_%"'
        )
        return [result[0] for result in self.cursor.fetchall()]

    @_connect
    def getColumnNames(self, table: str) -> list[str]:
        """Return a list of column names from a table."""
        self.cursor.execute(f"select * from {table} where 1=0")
        return [description[0] for description in self.cursor.description]

    @_connect
    def count(
        self,
        table: str,
        matchCriteria: list[tuple] | dict = None,
        exactMatch: bool = True,
    ) -> int:
        """Return number of items in table.

        :param matchCriteria: Can be a list of 2-tuples where each
        tuple is (columnName, rowValue) or a dictionary where
        keys are column names and values are row values.
        If None, all rows from the table will be counted.

        :param exactMatch: If False, the row value for a give column
        in matchCriteria will be matched as a substring. Has no effect if
        matchCriteria is None.
        """
        statement = f"select count(_rowid_) from {table}"
        try:
            if matchCriteria:
                self.cursor.execute(
                    f"{statement} where {self._getConditions(matchCriteria, exactMatch)}"
                )
            else:
                self.cursor.execute(f"{statement}")
            return self.cursor.fetchone()[0]
        except:
            return 0

    @_connect
    def addToTable(self, table: str, values: tuple[any], columns: tuple[str] = None):
        """Add row of values to table.

        :param table: The table to insert into.

        :param values: A tuple of values to be inserted into the table.

        :param columns: If None, values param is expected to supply
        a value for every column in the table. If columns is
        provided, it should contain the same number of elements as values."""
        parameterizer = ", ".join("?" for _ in values)
        loggerValues = ", ".join(str(value) for value in values)
        try:
            if columns:
                columns = ", ".join(column for column in columns)
                self.cursor.execute(
                    f"insert into {table} ({columns}) values({parameterizer})", values
                )
            else:
                self.cursor.execute(
                    f"insert into {table} values({parameterizer})", values
                )
            self.logger.info(f'Added "{loggerValues}" to {table} table.')
        except Exception as e:
            if "constraint" not in str(e).lower():
                self.logger.exception(
                    f'Error adding "{loggerValues}" to {table} table.'
                )
            else:
                self.logger.debug(str(e))

    @_connect
    def getRows(
        self,
        table: str,
        matchCriteria: list[tuple] | dict = None,
        exactMatch: bool = True,
        sortByColumn: str = None,
        columnsToReturn: list[str] = None,
        valuesOnly: bool = False,
    ) -> tuple[dict] | tuple[tuple]:
        """Returns rows from table as a list of dictionaries
        where the key-value pairs of the dictionaries are
        column name: row value.

        :param matchCriteria: Can be a list of 2-tuples where each
        tuple is (columnName, rowValue) or a dictionary where
        keys are column names and values are row values.

        :param exactMatch: If False, the rowValue for a give column
        will be matched as a substring.

        :param sortByColumn: A column name to sort the results by.

        :param columnsToReturn: Optional list of column names.
        If provided, the dictionaries returned by getRows() will
        only contain the provided columns. Otherwise every column
        in the row is returned.

        :param valuesOnly: Return the results as a tuple of tuples
        instead of a tuple of dictionaries that have column names as keys.
        The results will still be sorted according to sortByColumn if
        one is provided.
        """
        statement = f"select * from {table}"
        matches = []
        if not matchCriteria:
            self.cursor.execute(statement)
        else:
            self.cursor.execute(
                f"{statement} where {self._getConditions(matchCriteria, exactMatch)}"
            )
        matches = self.cursor.fetchall()
        results = tuple(
            self._getDict(table, match, columnsToReturn) for match in matches
        )
        if sortByColumn:
            results = tuple(sorted(results, key=lambda x: x[sortByColumn]))
        if valuesOnly:
            return tuple(tuple(row.values()) for row in results)
        else:
            return results

    @_connect
    def find(
        self, table: str, queryString: str, columns: list[str] = None
    ) -> tuple[dict]:
        """Search for rows that contain queryString as a substring
        of any column.

        :param table: The table to search.

        :param queryString: The substring to search for in all columns.

        :param columns: A list of columns to search for queryString.
        If None, all columns in the table will be searched.
        """
        results = []
        if not columns:
            columns = self.getColumnNames(table)
        for column in columns:
            results.extend(
                [
                    row
                    for row in self.getRows(
                        table, [(column, queryString)], exactMatch=False
                    )
                    if row not in results
                ]
            )
        return tuple(results)

    @_connect
    def delete(
        self, table: str, matchCriteria: list[tuple] | dict, exactMatch: bool = True
    ) -> int:
        """Delete records from table.

        Returns number of deleted records.

        :param matchCriteria: Can be a list of 2-tuples where each
        tuple is (columnName, rowValue) or a dictionary where
        keys are column names and values are row values.

        :param exactMatch: If False, the rowValue for a give column
        will be matched as a substring.
        """
        numMatches = self.count(table, matchCriteria, exactMatch)
        conditions = self._getConditions(matchCriteria, exactMatch)
        try:
            self.cursor.execute(f"delete from {table} where {conditions}")
            self.logger.info(
                f'Deleted {numMatches} from "{table}" where {conditions}".'
            )
            return numMatches
        except Exception as e:
            self.logger.debug(f'Error deleting from "{table}" where {conditions}.\n{e}')
            return 0

    @_connect
    def update(
        self,
        table: str,
        columnToUpdate: str,
        newValue: Any,
        matchCriteria: list[tuple] | dict = None,
    ) -> bool:
        """Update row value for entry matched with matchCriteria.

        :param columnToUpdate: The column to be updated in the matched row.

        :param newValue: The new value to insert.

        :param matchCriteria: Can be a list of 2-tuples where each
        tuple is (columnName, rowValue) or a dictionary where
        keys are column names and values are row values.
        If None, every row will be updated.

        Returns True if successful, False if not."""
        statement = f"update {table} set {columnToUpdate} = ?"
        if matchCriteria:
            if self.count(table, matchCriteria) == 0:
                self.logger.info(
                    f"Couldn't find matching records in {table} table to update to '{newValue}'"
                )
                return False
            conditions = self._getConditions(matchCriteria)
            statement += f" where {conditions}"
        else:
            conditions = None
        try:
            self.cursor.execute(
                statement,
                (newValue,),
            )
            self.logger.info(
                f'Updated "{columnToUpdate}" in "{table}" table to "{newValue}" where {conditions}'
            )
            return True
        except UnboundLocalError:
            tableFilterString = "\n".join(tableFilter for tableFilter in matchCriteria)
            self.logger.error(f"No records found matching filters: {tableFilterString}")
            return False
        except Exception as e:
            self.logger.error(
                f'Failed to update "{columnToUpdate}" in "{table}" table to "{newValue}" where {conditions}"\n{e}'
            )
            return False

    @_connect
    def dropTable(self, table: str) -> bool:
        """Drop a table from the database.

        Returns True if successful, False if not."""
        try:
            self.cursor.execute(f"drop Table {table}")
            self.logger.info(f'Dropped table "{table}"')
        except Exception as e:
            print(e)
            self.logger.error(f'Failed to drop table "{table}"')

    @_connect
    def addColumn(self, table: str, column: str, _type: str, defaultValue: str = None):
        """Add a new column to table.

        :param column: Name of the column to add.

        :param _type: The data type of the new column.

        :param defaultValue: Optional default value for the column."""
        try:
            if defaultValue:
                self.cursor.execute(
                    f"alter table {table} add column {column} {_type} default {defaultValue}"
                )
            else:
                self.cursor.execute(f"alter table {table} add column {column} {_type}")
            self.logger.info(f'Added column "{column}" to "{table}" table.')
        except Exception as e:
            self.logger.error(f'Failed to add column "{column}" to "{table}" table.')


def dataToString(data: list[dict], sortKey: str = None) -> str:
    """Uses tabulate to produce pretty string output
    from a list of dictionaries.

    :param data: Assumes all dictionaries in list have the same set of keys.

    :param sortKey: Optional dictionary key to sort data with."""
    if len(data) == 0:
        return ""
    if sortKey:
        data = sorted(data, key=lambda d: d[sortKey])
    for i, d in enumerate(data):
        for k in d:
            data[i][k] = str(data[i][k])
    terminalWidth = os.get_terminal_size().columns
    maxColWidths = terminalWidth
    """ Reducing the column width by tabulating one row at a time
    and then reducing further by tabulating the whole set proved to be 
    faster than going straight to tabulating the whole set and reducing
    the column width."""
    tooWide = True
    while tooWide and maxColWidths > 1:
        for i, row in enumerate(data):
            output = tabulate(
                [row],
                headers="keys",
                disable_numparse=True,
                tablefmt="grid",
                maxcolwidths=maxColWidths,
            )
            if output.index("\n") > terminalWidth:
                maxColWidths -= 2
                tooWide = True
                break
            tooWide = False
    output = tabulate(
        data,
        headers="keys",
        disable_numparse=True,
        tablefmt="grid",
        maxcolwidths=maxColWidths,
    )
    # trim max column width until the output string is less wide than the current terminal width.
    while output.index("\n") > terminalWidth and maxColWidths > 1:
        maxColWidths -= 2
        maxColWidths = max(1, maxColWidths)
        output = tabulate(
            data,
            headers="keys",
            disable_numparse=True,
            tablefmt="grid",
            maxcolwidths=maxColWidths,
        )
    return output
