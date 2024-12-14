from model import Model
from view import View

class Controller:
    def __init__(self):
        self.model = Model()
        self.view = View()

    def run(self):
        while True:
            choice = self.view.show_main_menu()
            if choice == '1':
                self.handle_table("Author")
            elif choice == '2':
                self.handle_table("Collection")
            elif choice == '3':
                self.handle_table("Edition")
            elif choice == '4':
                self.handle_table("Author_Collection_Edition")
            elif choice == '5':
                self.handle_table("Author_Collection_Edition_ED")
            elif choice == '6':
                self.advanced_search()
            elif choice == '7':
                self.view.show_message("Exiting the program. Goodbye!")
                break
            else:
                self.view.show_message("Invalid choice. Please try again.")

    def handle_table(self, table_name):
        while True:
            choice = self.view.show_table_menu(table_name)
            if table_name in ["Author", "Collection", "Edition"]:
                if choice == '1':
                    self.add(table_name)
                elif choice == '2':
                    self.view_table(table_name)
                elif choice == '3':
                    self.update(table_name)
                elif choice == '4':
                    self.delete(table_name)
                elif choice == '5':
                    self.generate_data(table_name)
                elif choice == '6':
                    break
                else:
                    self.view.show_message("Invalid choice. Please try again.")
            else:
                # For linking tables (Author_Collection_Edition, Author_Collection_Edition_ED)
                if choice == '1':
                    self.add(table_name)
                elif choice == '2':
                    self.view_table(table_name)
                elif choice == '3':
                    self.delete(table_name, composite=True)
                elif choice == '4':
                    self.generate_data(table_name)
                elif choice == '5':
                    break
                else:
                    self.view.show_message("Invalid choice. Please try again.")

    def add(self, table_name):
        data = self.view.get_data_input(table_name)
        if data:
            self.model.add_data(table_name, data)

    def view_table(self, table_name):
        data = self.model.get_all(table_name)
        if table_name == "Author":
            self.view.show_author(data)
        elif table_name == "Collection":
            self.view.show_collection(data)
        elif table_name == "Edition":
            self.view.show_edition(data)
        elif table_name == "Author_Collection_Edition":
            self.view.show_author_collection_edition(data)
        elif table_name == "Author_Collection_Edition_ED":
            self.view.show_author_collection_edition_ed(data)

    def update(self, table_name):
        pk, val = self.view.get_pk(table_name)
        new_data = self.view.get_update_input(table_name, val)
        if new_data:
            self.model.update_data(table_name, new_data, pk, val)

    def delete(self, table_name, composite=False):
        if composite:
            conditions = self.view.get_pk_composite(table_name)
            if conditions:
                self.model.delete_data_composite(table_name, conditions)
        else:
            pk, val = self.view.get_pk(table_name)
            self.model.delete_data(table_name, pk, val)

    def generate_data(self, table_name):
        num = self.view.get_num()
        self.model.generate_data(table_name, num)

    def advanced_search(self):
        inputs = self.view.advanced_search_input()
        if inputs:
            surname_pattern, min_pages, max_pages, start_date, end_date = inputs
            data = self.model.advanced_search(surname_pattern, min_pages, max_pages, start_date, end_date)
            self.view.show_advanced_search_results(data)
