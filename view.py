from tabulate import tabulate

class View:
    def show_author(self, data):
        print("Authors:")
        headers = ["author_id", "name", "surname"]
        print(tabulate(data, headers, tablefmt="pretty"))

    def show_collection(self, data):
        print("Collections:")
        headers = ["collection_id", "name", "type", "view"]
        print(tabulate(data, headers, tablefmt="pretty"))

    def show_edition(self, data):
        print("Editions:")
        headers = ["edition_id", "name", "branch", "number_of_pages", "languages"]
        print(tabulate(data, headers, tablefmt="pretty"))

    def show_author_collection_edition(self, data):
        print("Author_Collection_Edition:")
        headers = ["author_id", "edition_id"]
        print(tabulate(data, headers, tablefmt="pretty"))

    def show_author_collection_edition_ed(self, data):
        print("Author_Collection_Edition_ED:")
        headers = ["edition_id", "collection_id", "date"]
        print(tabulate(data, headers, tablefmt="pretty"))

    def show_message(self, message):
        print(message)

    def show_main_menu(self):
        self.show_message("\nWelcome! Which table do you want to work with?")
        self.show_message("1. Author")
        self.show_message("2. Collection")
        self.show_message("3. Edition")
        self.show_message("4. Author_Collection_Edition")
        self.show_message("5. Author_Collection_Edition_ED")
        self.show_message("6. Advanced Search")
        self.show_message("7. Quit")
        return input("Enter your choice: ")

    def show_table_menu(self, table_name):
        self.show_message(f"\nWorking with {table_name}")
        self.show_message("1. Add")
        self.show_message("2. View")
        if table_name not in ["Author_Collection_Edition", "Author_Collection_Edition_ED"]:
            self.show_message("3. Update")
            self.show_message("4. Delete")
            self.show_message("5. Generate data")
            self.show_message("6. Back")
        else:
            # Linking tables might be add/view/delete/generate only
            self.show_message("3. Delete")
            self.show_message("4. Generate data")
            self.show_message("5. Back")
        return input("Enter your choice: ")

    def get_data_input(self, table_name):
        if table_name == "Author":
            name = input("Enter author's name: ")
            surname = input("Enter author's surname: ")
            return {"name": name, "surname": surname}
        elif table_name == "Collection":
            cname = input("Enter collection's name: ")
            ctype = input("Enter collection's type: ")
            cview = input("Enter collection's view: ")
            return {"name": cname, "type": ctype, "view": cview}
        elif table_name == "Edition":
            ename = input("Enter edition's name: ")
            branch = input("Enter edition's branch: ")
            try:
                pages = int(input("Enter number_of_pages: "))
            except ValueError:
                self.show_message("Invalid number for pages. Please enter an integer.")
                return self.get_data_input(table_name)
            langs = input("Enter languages (comma-separated): ")
            return {"name": ename, "branch": branch, "number_of_pages": pages, "languages": langs}
        elif table_name == "Author_Collection_Edition":
            try:
                aid = int(input("Enter Author_ID: "))
                eid = int(input("Enter Edition_ID: "))
            except ValueError:
                self.show_message("Invalid input. Author_ID and Edition_ID should be integers.")
                return self.get_data_input(table_name)
            return {"author_id": aid, "edition_id": eid}
        elif table_name == "Author_Collection_Edition_ED":
            try:
                eid = int(input("Enter Edition_ID: "))
                cid = int(input("Enter Collection_ID: "))
            except ValueError:
                self.show_message("Invalid input. Edition_ID and Collection_ID should be integers.")
                return self.get_data_input(table_name)
            date = input("Enter Date (YYYY-MM-DD): ")
            return {"edition_id": eid, "collection_id": cid, "date": date}

    def get_update_input(self, table_name, pk_val):
        if table_name == "Author":
            name = input("Enter new author's name: ")
            surname = input("Enter new author's surname: ")
            return {"name": name, "surname": surname}
        elif table_name == "Collection":
            cname = input("Enter new collection's name: ")
            ctype = input("Enter new collection's type: ")
            cview = input("Enter new collection's view: ")
            return {"name": cname, "type": ctype, "view": cview}
        elif table_name == "Edition":
            ename = input("Enter new edition's name: ")
            branch = input("Enter new branch: ")
            try:
                pages = int(input("Enter new number_of_pages: "))
            except ValueError:
                self.show_message("Invalid number for pages. Please enter an integer.")
                return self.get_update_input(table_name, pk_val)
            langs = input("Enter new languages (comma-separated): ")
            return {"name": ename, "branch": branch, "number_of_pages": pages, "languages": langs}

    def get_pk(self, table_name):
        if table_name == "Author":
            try:
                val = int(input("Enter author_id: "))
            except ValueError:
                self.show_message("Invalid Author_ID. It should be an integer.")
                return self.get_pk(table_name)
            return "author_id", val
        elif table_name == "Collection":
            try:
                val = int(input("Enter collection_id: "))
            except ValueError:
                self.show_message("Invalid Collection_ID. It should be an integer.")
                return self.get_pk(table_name)
            return "collection_id", val
        elif table_name == "Edition":
            try:
                val = int(input("Enter edition_id: "))
            except ValueError:
                self.show_message("Invalid Edition_ID. It should be an integer.")
                return self.get_pk(table_name)
            return "edition_id", val

    def get_pk_composite(self, table_name):
        if table_name == "Author_Collection_Edition":
            try:
                aid = int(input("Enter Author_ID: "))
                eid = int(input("Enter Edition_ID: "))
            except ValueError:
                self.show_message("Invalid input. Author_ID and Edition_ID should be integers.")
                return self.get_pk_composite(table_name)
            return [("author_id", aid), ("edition_id", eid)]
        elif table_name == "Author_Collection_Edition_ED":
            try:
                eid = int(input("Enter Edition_ID: "))
                cid = int(input("Enter Collection_ID: "))
            except ValueError:
                self.show_message("Invalid input. Edition_ID and Collection_ID should be integers.")
                return self.get_pk_composite(table_name)
            return [("edition_id", eid), ("collection_id", cid)]

    def get_num(self):
        try:
            return int(input("Enter number of data to generate: "))
        except ValueError:
            self.show_message("Invalid number. Please enter an integer.")
            return self.get_num()

    def advanced_search_input(self):
        surname_pattern = input("Enter surname pattern (e.g., part of surname): ")
        try:
            min_pages = int(input("Enter minimum number_of_pages: "))
            max_pages = int(input("Enter maximum number_of_pages: "))
        except ValueError:
            self.show_message("Invalid input for pages. Please enter integers.")
            return self.advanced_search_input()
        start_date = input("Enter start date (YYYY-MM-DD): ")
        end_date = input("Enter end date (YYYY-MM-DD): ")
        return surname_pattern, min_pages, max_pages, start_date, end_date

    def show_advanced_search_results(self, data):
        print("Advanced Search Results:")
        headers = ["Author Name", "Author Surname", "Edition Name", "Pages", "Date", "Collection Name"]
        print(tabulate(data, headers, tablefmt="pretty"))
