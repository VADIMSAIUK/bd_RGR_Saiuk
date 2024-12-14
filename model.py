import psycopg2
from view import View
import random
import string

class Model:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname='science_test',
            user='postgres',
            password='1111',
            host='localhost',
            port='5432'
        )
        self.view = View()
        # Create tables if not exist
        self.create_tables()

    def create_tables(self):
        c = self.conn.cursor()
        # Check and create Author table
        c.execute("""
            CREATE TABLE IF NOT EXISTS "Author" (
                "author_id" SERIAL PRIMARY KEY,
                "name" VARCHAR NOT NULL,
                "surname" VARCHAR NOT NULL
            );
        """)

        # Check and create Collection table
        c.execute("""
            CREATE TABLE IF NOT EXISTS "Collection" (
                "collection_id" SERIAL PRIMARY KEY,
                "name" VARCHAR NOT NULL,
                "type" VARCHAR NOT NULL,
                "view" VARCHAR NOT NULL
            );
        """)

        # Check and create Edition table
        c.execute("""
            CREATE TABLE IF NOT EXISTS "Edition" (
                "edition_id" SERIAL PRIMARY KEY,
                "name" VARCHAR NOT NULL,
                "branch" VARCHAR NOT NULL,
                "number_of_pages" INT NOT NULL,
                "languages" VARCHAR NOT NULL
            );
        """)

        # Check and create Author_Collection_Edition table
        c.execute("""
            CREATE TABLE IF NOT EXISTS "Author_Collection_Edition" (
                "author_id" INT NOT NULL,
                "edition_id" INT NOT NULL,
                PRIMARY KEY ("author_id", "edition_id"),
                FOREIGN KEY ("author_id") REFERENCES "Author" ("author_id") ON DELETE CASCADE,
                FOREIGN KEY ("edition_id") REFERENCES "Edition" ("edition_id") ON DELETE CASCADE
            );
        """)

        # Check and create Author_Collection_Edition_ED table
        c.execute("""
            CREATE TABLE IF NOT EXISTS "Author_Collection_Edition_ED" (
                "edition_id" INT NOT NULL,
                "collection_id" INT NOT NULL,
                "date" DATE,
                PRIMARY KEY ("edition_id", "collection_id"),
                FOREIGN KEY ("edition_id") REFERENCES "Edition" ("edition_id") ON DELETE CASCADE,
                FOREIGN KEY ("collection_id") REFERENCES "Collection" ("collection_id") ON DELETE CASCADE
            );
        """)

        self.conn.commit()

    def get_all(self, table_name):
        c = self.conn.cursor()
        c.execute(f'SELECT * FROM "{table_name}"')
        return c.fetchall()

    def add_data(self, table_name, data, pk_columns=None):
        try:
            c = self.conn.cursor()
            columns = ', '.join([f'"{key}"' for key in data.keys()])
            placeholders = ', '.join(['%s'] * len(data))
            sql = f'INSERT INTO "{table_name}" ({columns}) VALUES ({placeholders});'
            c.execute(sql, list(data.values()))
            self.conn.commit()
            self.view.show_message("Added successfully!")
        except psycopg2.errors.ForeignKeyViolation as e:
            self.conn.rollback()
            self.view.show_message(f"ERROR: Foreign key violation.\n{e}")
        except psycopg2.errors.UniqueViolation as e:
            self.conn.rollback()
            self.view.show_message(f"ERROR: Duplicate key.\n{e}")

    def update_data(self, table_name, data, condition_column, condition_value):
        try:
            if not table_name or not data or not condition_column or condition_value is None:
                self.view.show_message("Insufficient information to update data.")
                return
            c = self.conn.cursor()
            set_clause = ", ".join([f'"{key}" = %s' for key in data.keys()])
            sql = f'UPDATE "{table_name}" SET {set_clause} WHERE "{condition_column}" = %s;'
            values = list(data.values())
            values.append(condition_value)
            c.execute(sql, values)
            self.conn.commit()
            self.view.show_message("Updated successfully!")
        except psycopg2.Error as e:
            self.conn.rollback()
            self.view.show_message(str(e))

    def delete_data(self, table_name, cond, val):
        try:
            c = self.conn.cursor()
            sql = f'DELETE FROM "{table_name}" WHERE "{cond}" = %s;'
            c.execute(sql, (val,))
            self.conn.commit()
            self.view.show_message("Deleted successfully!")
        except psycopg2.Error as e:
            self.conn.rollback()
            self.view.show_message(str(e))

    def delete_data_composite(self, table_name, conditions):
        # conditions: list of tuples (cond, val)
        try:
            c = self.conn.cursor()
            where_clause = ' AND '.join([f'"{cond}" = %s' for cond, _ in conditions])
            values = [val for _, val in conditions]
            sql = f'DELETE FROM "{table_name}" WHERE {where_clause};'
            c.execute(sql, values)
            self.conn.commit()
            self.view.show_message("Deleted successfully!")
        except psycopg2.Error as e:
            self.conn.rollback()
            self.view.show_message(str(e))

    def generate_data(self, table_name, num):
        # Example random data generation
        c = self.conn.cursor()
        try:
            if table_name == "Author":
                # Insert random authors
                for _ in range(num):
                    name = ''.join(random.choice(string.ascii_letters) for _ in range(5))
                    surname = ''.join(random.choice(string.ascii_letters) for _ in range(7))
                    c.execute('INSERT INTO "Author" ("name","surname") VALUES (%s,%s)', (name, surname))

            elif table_name == "Collection":
                for _ in range(num):
                    col_name = ''.join(random.choice(string.ascii_letters) for _ in range(8))
                    col_type = random.choice(["Literature", "Fiction", "Poetry", "Science"])
                    col_view = random.choice(["Paper", "Electronic"])
                    c.execute('INSERT INTO "Collection" ("name","type","view") VALUES (%s,%s,%s)', (col_name, col_type, col_view))

            elif table_name == "Edition":
                for _ in range(num):
                    ed_name = ''.join(random.choice(string.ascii_letters) for _ in range(6))
                    branch = random.choice(["Fiction", "Poetry", "Non-Fiction", "Science"])
                    pages = random.randint(50, 1000)
                    languages = random.choice(["English", "Ukrainian", "French"])
                    c.execute('INSERT INTO "Edition" ("name","branch","number_of_pages","languages") VALUES (%s,%s,%s,%s)', (ed_name, branch, pages, languages))

            elif table_name == "Author_Collection_Edition":
                # For linking table, we need existing authors and editions
                c.execute('SELECT "author_id" FROM "Author"')
                authors = [row[0] for row in c.fetchall()]
                c.execute('SELECT "edition_id" FROM "Edition"')
                editions = [row[0] for row in c.fetchall()]
                if not authors or not editions:
                    self.view.show_message("No authors or editions to link. Generate them first.")
                    return
                for _ in range(num):
                    aid = random.choice(authors)
                    eid = random.choice(editions)
                    try:
                        c.execute('INSERT INTO "Author_Collection_Edition" ("author_id","edition_id") VALUES (%s,%s)', (aid, eid))
                    except psycopg2.errors.UniqueViolation:
                        self.conn.rollback()  # ignore duplicates
                        continue

            elif table_name == "Author_Collection_Edition_ED":
                # We need existing editions and collections
                c.execute('SELECT "edition_id" FROM "Edition"')
                editions = [row[0] for row in c.fetchall()]
                c.execute('SELECT "collection_id" FROM "Collection"')
                collections = [row[0] for row in c.fetchall()]
                if not editions or not collections:
                    self.view.show_message("No editions or collections to link. Generate them first.")
                    return
                for _ in range(num):
                    eid = random.choice(editions)
                    cid = random.choice(collections)
                    # Random date
                    year = random.randint(1800, 2020)
                    month = random.randint(1,12)
                    day = random.randint(1,28)
                    date_str = f"{year}-{month:02d}-{day:02d}"
                    try:
                        c.execute('INSERT INTO "Author_Collection_Edition_ED" ("edition_id","collection_id","date") VALUES (%s,%s,%s)', (eid, cid, date_str))
                    except psycopg2.errors.UniqueViolation:
                        self.conn.rollback()
                        continue

            self.conn.commit()
            self.view.show_message("Generated successfully!")
        except psycopg2.Error as e:
            self.conn.rollback()
            self.view.show_message(str(e))

    def advanced_search(self, surname_pattern, min_pages, max_pages, start_date, end_date):
        # Example advanced query
        # Joins across multiple tables and applies filters on numeric range, string pattern, and date range
        c = self.conn.cursor()
        sql = """
            SELECT a.name, a.surname, ed.name as edition_name, ed.number_of_pages, aced.date, c.name as collection_name
            FROM "Author" a
            JOIN "Author_Collection_Edition" ace ON a.author_id = ace.author_id
            JOIN "Edition" ed ON ace.edition_id = ed.edition_id
            JOIN "Author_Collection_Edition_ED" aced ON ed.edition_id = aced.edition_id
            JOIN "Collection" c ON aced.collection_id = c.collection_id
            WHERE a.surname ILIKE %s
              AND ed.number_of_pages BETWEEN %s AND %s
              AND aced.date BETWEEN %s AND %s
            ORDER BY a.surname, ed.number_of_pages;
        """
        c.execute(sql, (f"%{surname_pattern}%", min_pages, max_pages, start_date, end_date))
        rows = c.fetchall()
        return rows
6
