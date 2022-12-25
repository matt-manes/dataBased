import argparse
import sys
from pathlib import Path

from dataBased import DataBased, dataToString


def getArgs() -> argparse.Namespace:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-db",
        "--dbName",
        type=str,
        default=None,
        help="""Name of database file to use.""",
    )

    parser.add_argument(
        "-i",
        "--info",
        action="store_true",
        help=""" Display table names, their respective columns, and how many records they contain.
        If a -t/--tables arg is passed, just the columns and row count for those tables will be shown.""",
    )

    parser.add_argument(
        "-t",
        "--tables",
        type=str,
        nargs="*",
        default=[],
        help="""Limits commands to a specific list of tables.
        Optional for some commands, required for others.""",
    )

    parser.add_argument(
        "-c",
        "--columns",
        type=str,
        nargs="*",
        default=[],
        help=""" Limits commands to a specific list of columns.
        Optional for some commands, required for others. """,
    )

    parser.add_argument(
        "-f",
        "--find",
        type=str,
        default=None,
        help=""" A substring to search the database for. 
        If a -c/--columns arg(s) is not given, the values will be matched against all columns.
        Similarly, if a -t/--tables arg(s) is not given, the values will be searched for in all tables.
        To print an entire table, pass an empty space in quote marks:
        -f " " -t tableName""",
    )

    parser.add_argument(
        "-d",
        "--delete",
        type=str,
        nargs="*",
        default=[],
        help=""" A list of values to be deleted from the database.
        A -c/--columns arg must be supplied.
        A -t/--tables arg must be supplied.""",
    )

    parser.add_argument(
        "-u",
        "--update",
        type=str,
        default=None,
        nargs=2,
        help=""" Update a record in the database.
        Expects two arguments: the current value and the new value.
        A -c/--columns arg must be supplied.
        A -t/--tables arg must be supplied.""",
    )

    parser.add_argument(
        "-sb", "--sortBy", type=str, default=None, help="Column to sort results by."
    )

    args = parser.parse_args()

    if not Path(args.dbName).exists():
        raise Exception(f"{args.dbName} does not exist.")

    return args


if __name__ == "__main__":
    args = getArgs()
    sys.tracebacklimit = 0

    with DataBased(dbPath=args.dbName) as db:
        if args.info:
            print("Getting database info...")
            print()
            if not args.tables:
                args.tables = db.getTableNames()
            results = []
            for table in args.tables:
                count = db.count(table)
                columns = db.getColumnNames(table)
                results.append(
                    {
                        "table name": table,
                        "columns": ", ".join(columns),
                        "number of rows": count,
                    }
                )
            if args.sortBy and args.sortBy in results[0]:
                results = sorted(results, key=lambda x: x[args.sortBy])
            print(dataToString(results))

        if args.find:
            print("Finding records... ")
            print()
            if not args.tables:
                args.tables = db.getTableNames()
            for table in args.tables:
                results = db.find(table, args.find, args.columns)
                if args.sortBy and args.sortBy in results[0]:
                    results = sorted(results, key=lambda x: x[args.sortBy])
                if args.columns:
                    print(
                        f"{len(results)} results for '{args.find}' in '{', '.join(args.columns)}' column(s) of '{table}' table:"
                    )
                else:
                    print(
                        f"{len(results)} results for '{args.find}' in '{table}' table:"
                    )
                print(dataToString(results))
                print()

        if args.delete:
            if not args.tables:
                raise ValueError("Missing -t/--tables arg for -d/--delete function.")
            if not args.columns:
                raise ValueError("Missing -c/--columns arg for -d/--delete function.")
            print("Deleting records... ")
            print()
            numDeletedRecords = 0
            failedDeletions = []
            for item in args.delete:
                success = db.delete(args.tables[0], [(args.columns[0], item)])
                if success:
                    numDeletedRecords += success
                else:
                    failedDeletions.append(item)
            print(
                f"Deleted {numDeletedRecords} record(s) from '{args.tables[0]}' table."
            )
            if len(failedDeletions) > 0:
                print(
                    f"Failed to delete the following {len(failedDeletions)} record(s) from '{args.tables[0]}' table:"
                )
                for fail in failedDeletions:
                    print(f"  {fail}")

        if args.update:
            if not args.tables:
                raise ValueError("Missing -t/--tables arg for -u/--update function.")
            if not args.columns:
                raise ValueError("Missing -c/--columns arg for -u/--update function.")
            print("Updating record... ")
            print()
            if db.update(
                args.tables[0],
                args.columns[0],
                args.update[1],
                [(args.columns[0], args.update[0])],
            ):
                print(
                    f"Updated '{args.columns[0]}' column from '{args.update[0]}' to '{args.update[1]}' in '{args.tables[0]}' table."
                )
            else:
                print(
                    f"Failed to update '{args.columns[0]}' column from '{args.update[0]}' to '{args.update[1]}' in '{args.tables[0]}' table."
                )
