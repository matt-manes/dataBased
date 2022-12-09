# dataBased
Install with
<pre>
python -m pip install git+https://github.com/matt-manes/dataBased
</pre>
Git must be installed and in your PATH.<br><br>
dataBased is a package that wraps the standard library sqlite3 module to avoid writing queries except for table definitions<br>
It consists of the class DataBased and an additional function for displaying information called dataToString()<br>
The DataBased class contains functions for creating databases and tables; inserting, updating, and deleting rows; 
as well as retrieving data and schema information.<br>
Member functions that require a database connection will
automatically create one when called if one isn't already open,
but a manual call to self.close() needs to be called in order to
save the database file and release the connection.<br>
Usage:<br>
<pre>
from dataBased import DataBased
from datetime import datetime
#if the .db file specified doesn't exist, it will be created
#a log file with the same name will be generated and stored in the same directory
db = DataBased(dbPath='path/to/records.db')
tables = ['kitchenTables(numLegs int, topMaterial text, shape text, dateAdded timestamp)']
#A table will only be created if it doesn't exist. createTables() will not overwrite an existing table.
db.createTables(tables)
kitchenTables = [(4, 'birch', 'round', datetime.now()),
                 (3, 'oak', 'round', datetime.now()),
                 (6, 'granite', 'rectangle', datetime.now())]
for kitchenTable in kitchenTables:
    db.addToTable('kitchenTables', kitchenTable)
    
print(db.count('kitchenTables'))
print(db.getTableNames())
print(db.getColumnNames('kitchenTables'))
print(db.getRows('kitchenTables', [('numLegs', 6)]))
print(db.getRows('kitchenTables', [('shape', 'round')], sortByColumn='numLegs'))
print(db.getRows('kitchenTables', [('shape', 'round'), ('numLegs', 4)]))

db.update('kitchenTables', [('numLegs', 3)], columnToUpdate='topMaterial', newValue='glass')
print(db.getRows('kitchenTables', sortByColumn='numLegs'))
db.close()
</pre>
produces:
<pre>
number of rows: 3
table names: ['kitchenTables']
column names: ['numLegs', 'topMaterial', 'shape', 'dateAdded']
[{'numLegs': 6, 'topMaterial': 'granite', 'shape': 'rectangle', 'dateAdded': datetime.datetime(2022, 12, 9, 15, 56, 56, 543549)}]
[{'numLegs': 3, 'topMaterial': 'oak', 'shape': 'round', 'dateAdded': datetime.datetime(2022, 12, 9, 15, 56, 56, 543549)}, {'numLegs': 4, 'topMaterial': 'birch', 'shape': 'round', 'dateAdded': datetime.datetime(2022, 12, 9, 15, 56, 56, 543549)}]
[{'numLegs': 4, 'topMaterial': 'birch', 'shape': 'round', 'dateAdded': datetime.datetime(2022, 12, 9, 15, 56, 56, 543549)}]
[{'numLegs': 3, 'topMaterial': 'glass', 'shape': 'round', 'dateAdded': datetime.datetime(2022, 12, 9, 15, 56, 56, 543549)}, {'numLegs': 4, 'topMaterial': 'birch', 'shape': 
'round', 'dateAdded': datetime.datetime(2022, 12, 9, 15, 56, 56, 543549)}, {'numLegs': 6, 'topMaterial': 'granite', 'shape': 'rectangle', 'dateAdded': datetime.datetime(2022, 12, 9, 15, 56, 56, 543549)}]
</pre>
