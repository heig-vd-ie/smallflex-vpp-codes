from __future__ import annotations
import sqlite3


if __name__ == "__main__":
    db_file = ".cache/database.sqlite"
    with open('src/schema/schema.sql', 'r') as sql_file:
        sql_script = sql_file.read()

    db = sqlite3.connect(db_file)
    cursor = db.cursor()
    cursor.executescript(sql_script)
    db.commit()
    db.close()

    con = sqlite3.connect(db_file)

    cur = con.cursor()

    # Return all results of query
    # cur.execute('select * from Hp')
