from datetime import datetime
import logging
import os
from pathlib import Path
import sqlite3
from tabulate import tabulate
from functools import wraps

class DataBased:
    """ Sqli wrapper so queries don't need to be written,
    except table definitions.\n
    Supports saving and reading dates as datetime objects."""
    def __init__(self, dbPath:str|Path, loggerEncoding:str='utf-8',
                 loggerMessageFormat:str='{levelname}|-|{asctime}|-|{message}'):
        """ 
        :param dbPath: String or Path object to database file.
        If a relative path is given, it will be relative to the
        current working directory. The log file will be saved to the
        same directory.\n
        :param loggerMessageFormat: '{' style format string
        for the logger object."""
        self.dbPath = Path(dbPath)
        self.dbName = Path(dbPath).name
        self._loggerInit(encoding=loggerEncoding,
                         messageFormat=loggerMessageFormat)
        self.connectionOpen = False
    
    def open(self):
        """ Open connection to db. """
        self.connection = sqlite3.connect(self.dbPath, 
                                          detect_types=sqlite3.PARSE_DECLTYPES|
                                          sqlite3.PARSE_COLNAMES)
        self.connection.execute('pragma foreign_keys = 1')
        self.cursor = self.connection.cursor()
        self.connectionOpen = True
    
    def close(self):
        """ Save and close connection to db.\n
        Call this as soon as you are done using the database if you have
        multiple threads or processes using the same database."""
        if self.connectionOpen:
            self.connection.commit()
            self.connection.close()
            self.connectionOpen = False
    
    def _connect(func):
        """ Decorator to open db connection if it isn't already open."""
        @wraps(func)
        def inner(*args, **kwargs):
            self = args[0]
            if not self.connectionOpen:
                self.open()
            results = func(*args, **kwargs)
            return results
        return inner
    
    def _loggerInit(self, messageFormat:str='{levelname}|-|{asctime}|-|{message}',
                    encoding:str='utf-8'):
        """ :param messageFormat: '{' style format string """
        self.logger = logging.getLogger(self.dbName)
        if not self.logger.hasHandlers():
            handler = logging.FileHandler(str(self.dbPath).replace('.','')+'.log', 
                                          encoding=encoding)
            handler.setFormatter(logging.Formatter(messageFormat, 
                                                    style='{', 
                                                    datefmt="%m/%d/%Y %I:%M:%S %p"))
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
    
    def _getDict(self, table:str, values:list)->dict:
        """ Converts the values of a row into a dictionary with column names as keys.\n
        :param table: The table that values were pulled from.\n
        :param values: List of values expected to be the same quantity
        and in the same order as the column names of table."""
        return {column:value for column,value in zip(self.getColumnNames(table), values)}
    
    def _getConditions(self, columnRows:list[tuple]|dict, exactMatch:bool=True)->str:
        """ Builds and returns the conditional portion of a query.\n
        :param columnRows: Can be a list of 2-tuples where each
        tuple is (columnName, rowValue) or a dictionary where
        keys are column names and values are row values.\n
        :param exactMatch: If False, the rowValue for a give column
        will be matched as a substring.\n
        Usage e.g.:\n
        self.cursor.execute(f'select * from {table} where {conditions}')"""
        if type(columnRows) == dict:
            columnRows = [(k,v) for k,v in columnRows.items()]
        if exactMatch:
            conditions = ' and '.join(f'"{columnRow[0]}" = "{columnRow[1]}"' 
                                      for columnRow in columnRows)
        else:
            conditions = ' and '.join(f'"{columnRow[0]}" like "%{columnRow[1]}%"' 
                                      for columnRow in columnRows)
        return f'({conditions})'
    
    @_connect
    def createTables(self, tableStatements:list[str]=[]):
        """ Create tables if they don't exist.\n
        :param tableStatements: Each statement should be
        in the form 'tableName(columnDefinitions)'"""
        if len(tableStatements) > 0:
            tableNames = self.getTableNames()
            for table in tableStatements:
                if table.split('(')[0].strip() not in tableNames:
                    self.cursor.execute(f'create table {table}')
                    self.logger.info(f'{table.split("(")[0]} table created.')
                else:
                    self.logger.info(f'{table.split("(")[0]} already exists.')
    
    @_connect
    def getTableNames(self)->list[str]:
        """ Returns a list of table names from database."""
        self.cursor.execute('select name from sqlite_Schema where type = "table" and name not like "sqlite_%"')
        return [result[0] for result in self.cursor.fetchall()]
    
    @_connect
    def getColumnNames(self, table:str)->list[str]:
        """ Return a list of column names from a table. """
        self.cursor.execute(f'select * from {table} where 1=0')
        return [description[0] for description in self.cursor.description]
    
    @_connect
    def count(self, table:str, columnRows:list[tuple]|dict=None, 
              exactMatch:bool=True)->int:
        """ Return number of items in table.\n
        :param columnRows: Can be a list of 2-tuples where each
        tuple is (columnName, rowValue) or a dictionary where
        keys are column names and values are row values.
        If None, all rows from the table will be counted.\n
        :param exactMatch: If False, the row value for a give column
        in columnRows will be matched as a substring. Has no effect if
        columnRows is None.\n"""
        statement = f'select count(_rowid_) from {table}'
        try:
            if columnRows:
                self.cursor.execute(f'{statement} where {self._getConditions(columnRows, exactMatch)}')
            else:
                self.cursor.execute(f'{statement}')
            return self.cursor.fetchone()[0]
        except:
            return 0
    
    @_connect
    def addToTable(self, table:str, values:tuple[any], columns:tuple[str]=None):
        """ Add row of values to table.\n
        :param table: The table to insert into.\n
        :param values: A tuple of values to be inserted into the table.\n
        :param columns: If None, values param is expected to supply
        a value for every column in the table. If columns is
        provided, it should contain the same number of elements as values."""
        parameterizer = ', '.join('?' for _ in values)
        loggerValues = ', '.join(str(value) for value in values)
        try:
            if columns:
                columns = ', '.join(column for column in columns)
                self.cursor.execute(f'insert into {table} ({columns}) values({parameterizer})', 
                                    values)
            else:
                self.cursor.execute(f'insert into {table} values({parameterizer})', values)
            self.logger.info(f'Added "{loggerValues}" to {table} table.')
        except Exception as e:
            if 'constraint' not in str(e).lower():
                self.logger.exception(f'Error adding "{loggerValues}" to {table} table.')
            else:
                self.logger.debug(str(e))
    
    @_connect
    def getRows(self, table:str, 
                        columnRows:list[tuple]|dict=None, 
                        exactMatch:bool=True)->list[dict]:
        """ Returns rows from table as a list of dictionaries
        where the key-value pairs of the dictionaries are
        column name: row value.\n
        :param columnRows: Can be a list of 2-tuples where each
        tuple is (columnName, rowValue) or a dictionary where
        keys are column names and values are row values.\n
        :param exactMatch: If False, the rowValue for a give column
        will be matched as a substring.\n"""
        statement = f'select * from {table}'
        matches = []
        if not columnRows:
            self.cursor.execute(statement)
        else:
            self.cursor.execute(f'{statement} where {self._getConditions(columnRows, exactMatch)}')
        matches = self.cursor.fetchall()
        return [self._getDict(table, match) for match in matches]
    
    @_connect
    def delete(self, table:str, columnRows:list[tuple]|dict, exactMatch:bool=True)->int:
        """ Delete records from table.\n
        Returns number of deleted records.\n
        :param columnRows: Can be a list of 2-tuples where each
        tuple is (columnName, rowValue) or a dictionary where
        keys are column names and values are row values.\n
        :param exactMatch: If False, the rowValue for a give column
        will be matched as a substring.\n"""
        numMatches = self.count(table, columnRows, exactMatch)
        conditions = self._getConditions(columnRows, exactMatch)
        try:
            self.cursor.execute(f'delete from {table} where {conditions}')
            self.logger.info(f'Deleted {numMatches} from "{table}" with conditions {conditions}".')
            return numMatches 
        except Exception as e:
            self.logger.debug(f'Error deleting from "{table}" with conditions {conditions}.\n{e}')
            return 0
    
    @_connect
    def update(self, table:str, columnRows:list[tuple], columnToUpdate:str, newValue:any)->bool:
        """ Update row value for entry matched with columnRows.\n
        :param columnRows: Can be a list of 2-tuples where each
        tuple is (columnName, rowValue) or a dictionary where
        keys are column names and values are row values.\n
        :param columnToUpdate: The column to be updated in the matched row.\n
        :param newValue: The new value to insert.\n
        Return True if successful, False if not."""
        conditions = self._getConditions(columnRows)
        try: 
            oldValue = self.getRows(table, columnRows, exactMatch=True)[0][columnToUpdate]
            self.cursor.execute(f'update {table} set {columnToUpdate} = ? where {conditions}',(newValue,))
            self.logger.info(f'Updated "{columnToUpdate}" in "{table}" table where {conditions} from "{oldValue}" to "{newValue}"')
            return True
        except UnboundLocalError:
            tableFilterString = "\n".join(tableFilter for tableFilter in columnRows)
            self.logger.error(f'No records found matching filters: {tableFilterString}')
            return False
        except Exception as e:
            self.logger.error(f'Failed to update "{columnToUpdate}" in "{table}" table where {conditions} from "{oldValue}" to "{newValue}"\n{e}')
            return False

def dataToString(data:list[dict], maxColWidths:int|list[int|None]=None, sortKey:str=None)->str:
    """ Uses tabulate to produce pretty string output
    from a list of dictionaries.\n
    :param data: Assumes all dictionaries in list have the same set of keys.\n
    :param maxColWidths: If None, the max width before wrapping will be set 
    according to the current terminal width.\n
    :param sortKey: Optional dictionary key to sort data with."""
    if not maxColWidths:
        terminalWidth = os.get_terminal_size().columns
        longestRowLength = 0
        longestString = 0
        for datum in data:
                rowLength = len(''.join(str(value) for value in datum.values()))
                if rowLength > longestRowLength:
                    longestRow = datum
                    longestRowLength = rowLength
                    longestString = max([len(str(value)) for value in datum.values()])
        i = longestString
        while longestRowLength > 0.75*terminalWidth:
            i -= 1
            longestRowLength = len(''.join(str(value)[:i] for value in longestRow.values()))
        maxColWidths = i
    if sortKey:
        data = sorted(data, key=lambda d:d[sortKey])
    for i,d in enumerate(data):
        for k in d:
            data[i][k] = str(data[i][k])
    return tabulate(data, headers='keys', disable_numparse=True, tablefmt='grid', maxcolwidths=maxColWidths)
