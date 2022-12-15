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
dataToString() uses the tabulate package (https://pypi.org/project/tabulate/) to print a list of dictionaries as a grid.<br>
Member functions that require a database connection will
automatically create one when called if one isn't already open,
but a manual call to self.close() needs to be called in order to
save the database file and release the connection.<br>
If a context manager is used, like in the following example, you don't need to worry about manually opening, saving, or closing the database.<br>
Usage:<br>
<pre>
from dataBased import DataBased
from datetime import datetime
#if the .db file specified doesn't exist, it will be created
#a log file with the same name will be generated and stored in the same directory
from dataBased import DataBased, dataToString
from datetime import datetime

# if the .db file specified doesn't exist, it will be created
# a log file with the same name will be generated and stored in the same directory
with DataBased(dbPath="records.db") as db:
    tables = [
        "kitchenTables(numLegs int, topMaterial text, shape text, dateAdded timestamp)"
    ]
    # A table will only be created if it doesn't exist. createTables() will not overwrite an existing table.
    db.createTables(tables)
    kitchenTables = [
        (4, "birch", "round", datetime.now()),
        (3, "oak", "round", datetime.now()),
        (6, "granite", "rectangle", datetime.now()),
    ]
    for kitchenTable in kitchenTables:
        db.addToTable("kitchenTables", kitchenTable)

    print(f'number of rows: {db.count("kitchenTables")}')
    print(f'table names: {db.getTableNames()}')
    print(f'column names: {db.getColumnNames("kitchenTables")}')
    print(db.getRows("kitchenTables", [("numLegs", 6)]))
    print(db.getRows("kitchenTables", [("shape", "round")], sortByColumn="numLegs"))
    print(db.getRows("kitchenTables", [("shape", "round"), ("numLegs", 4)]))

    db.update(
        "kitchenTables",
        columnToUpdate="topMaterial",
        newValue="glass",
        matchCriteria=[("numLegs", 3)],
    )
    print(db.getRows("kitchenTables", sortByColumn="numLegs"))
    print(dataToString(db.getRows("kitchenTables"), sortKey="topMaterial"))
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
and the final print() call on the dataToString() function produces:
<pre>
+-----------+---------------+-----------+----------------------------+
| numLegs   | topMaterial   | shape     | dateAdded                  |
+===========+===============+===========+============================+
| 4         | birch         | round     | 2022-12-14 18:19:31.501745 |
+-----------+---------------+-----------+----------------------------+
| 3         | glass         | round     | 2022-12-14 18:19:31.501745 |
+-----------+---------------+-----------+----------------------------+
| 6         | granite       | rectangle | 2022-12-14 18:19:31.501745 |
+-----------+---------------+-----------+----------------------------+
</pre>
