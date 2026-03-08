from __future__ import annotations

import re
import sqlite3
from tkinter import *
import tkinter.font as tk_font

class SimpleTable(Frame):
    def __init__(self, parent, rows=10, columns=2):
        # use black background so it "peeks through" to
        # form grid lines
        Frame.__init__(self, parent, background="black")
        self._widgets = []
        for row in range(rows):
            current_row = []
            for column in range(columns):
                label = Label(self, text="%s/%s" % (row, column),
                              borderwidth=0,
                              bg="lightblue" if row == 0 else ("white" if row % 2 == 1 else "light gray"),
                              width="20", wraplength="250", justify="left")
                label.grid(row=row, column=column, sticky="nsew", padx=1, pady=1)
                current_row.append(label)
            self._widgets.append(current_row)

        for column in range(columns):
            self.grid_columnconfigure(column, weight=1)


    def set(self, row, column, value):
        widget = self._widgets[row][column]
        widget.configure(text=value)


class NumberEntry(Entry):
    def __init__(self, master=None, decimal_digits: int = 0, value: int | None = 0, **kwargs):
        self.var = StringVar()
        self.decimal_digits: int = decimal_digits if decimal_digits > 0 else 0
        self.format: str = r"^[0-9]*(\.[0-9]{0,"+str(decimal_digits)+r"})?$" if decimal_digits > 0 else r"^[0-9]*$"
        self.var.set(format_number(value, decimal_digits))

        Entry.__init__(self, master, textvariable=self.var, **kwargs)
        self.old_value: str = self.var.get()
        self.var.trace('w', self.check)
        self.get_string, self.set = self.var.get, self.var.set

    def check(self, *_):
        if re.search(self.format, self.get_string(), flags=re.MULTILINE | re.UNICODE):
            self.old_value = self.get_string()
        else:
            # there's non-digit characters in the input; reject this
            self.set(self.old_value)

    def get(self) -> int | None:
        int_string = self.get_string()
        if int_string == "" or int_string == '.':
            return None
        if self.decimal_digits == 0:
            return int(self.get_string())
        int_string = "0" + int_string
        if int_string.count('.') == 0 or int_string.endswith("."):
            return int(int_string.removesuffix(".")) * (10 ** self.decimal_digits)
        front, back = int_string.split(".")
        back += "0" * (len(back) - self.decimal_digits)
        return int(front + back)

def format_number(value: int | None, decimal_digits: int) -> str:
    if value is None:
        return ""
    if decimal_digits < 1:
        return str(value)
    base_string = str(value)
    base_string = ((decimal_digits - len(base_string) + 1) * "0") + base_string
    return base_string[:-decimal_digits] + "." + base_string[-decimal_digits:]

def trim_after_format(value: int, decimal_digits: int) -> str:
    base_string = format_number(value, decimal_digits)
    base_string = base_string.rstrip("0")
    return base_string.rstrip(".")


find_spaces = re.compile(r"[ \t\n]", flags=re.MULTILINE | re.UNICODE)
find_non_letters = re.compile(r"[^a-zA-Z]", flags=re.MULTILINE | re.UNICODE)


def db_name(display_name: str) -> str:
    display_name = find_spaces.sub("_", display_name)
    return find_non_letters.sub("", display_name)


con = sqlite3.connect("data.sqlite", detect_types=sqlite3.PARSE_DECLTYPES)
sqlite3.register_adapter(bool, int)
sqlite3.register_converter("BOOLEAN", lambda v: bool(int(v)))


class Category:
    def __init__(self):
        self.id: int = -1
        self.db_name: str = ""
        self.display_name: str = ""
        self.column_text: str = ""
        # DB Col-Name to (Display Name, Primary key, Type, Decimal Digits, Incrementer)
        self.columns: dict[str, tuple[str, bool, str, int, bool]] = {}

    @classmethod
    def load_categories_from_db(cls) -> list[Category]:
        cur = con.cursor()
        res_query = cur.execute("SELECT COUNT(*) AS nums FROM sqlite_master WHERE type='table' AND name='master';").fetchone()
        if not res_query or res_query[0] < 1:
            cur.execute("CREATE TABLE master (ID INTEGER PRIMARY KEY AUTOINCREMENT, DB_NAME TEXT UNIQUE, NAME TEXT, COLUMNS TEXT);")
            return []
        res_query = cur.execute("SELECT ID, DB_NAME, NAME, COLUMNS FROM master ORDER BY ID;").fetchall()
        if res_query is None:
            return []
        categories = []
        for entry in res_query:
            cat = Category()
            cat.id, cat.db_name, cat.display_name, cat.column_text = entry
            cat._transform_text()
            categories.append(cat)
        return categories

    def _transform_text(self):
        if len(self.columns) == 0:
            self.columns = eval(self.column_text)
        else:
            self.column_text = str(self.columns)

    def add_category(self) -> bool:
        self._transform_text()
        try:
            with con as cur:
                cur.execute("INSERT INTO master(DB_NAME, NAME, COLUMNS) VALUES(?, ?, ?)",
                            (self.db_name, self.display_name, self.column_text,))
                cur.execute(self._try_create_table())
        except sqlite3.Error:
            return False
        return True

    def _try_create_table(self):
        tab_creator = f"CREATE TABLE {self.db_name} (" + \
                      ", ".join([f"{key} {val[2]}" for key, val in self.columns.items()]) + ", PRIMARY KEY ("
        for key, val in self.columns.items():
            _, prim, *_ = val
            if prim:
                tab_creator += key + ", "
        return tab_creator.removesuffix(", ") + "));"


class CategoryCreator:
    def __init__(self, base:BaseInterface):
        self.base = base
        self.options = ["TEXT", "INTEGER", "BOOLEAN"]

        self.master = Toplevel()
        self.master.title("Create Category")
        self.master.minsize(550, 400)
        self.master.focus_force()

        outer = Frame(self.master)

        canvas = Canvas(outer)
        scrollbar = Scrollbar(outer, orient="vertical", command=canvas.yview)

        self.scroll_frame = Frame(canvas)

        self.scroll_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas.create_window((0, 0), window=self.scroll_frame, anchor="nw")

        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        def on_mousewheel(event):
            canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

        def on_mousewheel_linux_up(_):
            canvas.yview_scroll(-1, "units")

        def on_mousewheel_linux_down(_):
            canvas.yview_scroll(1, "units")

        self.scroll_frame.bind("<Enter>", lambda e: canvas.bind_all("<MouseWheel>", on_mousewheel))
        self.scroll_frame.bind("<Leave>", lambda e: canvas.unbind_all("<MouseWheel>"))

        canvas.bind_all("<Button-4>", on_mousewheel_linux_up)
        canvas.bind_all("<Button-5>", on_mousewheel_linux_down)

        self.groups = []
        self.add_group()

        controls = Frame(self.master)
        category_info = Frame(self.master)
        category_info.pack(fill="x")
        outer.pack(fill="both", expand=True)
        controls.pack(fill="x")

        self.category_name_entry = Entry(category_info)
        Label(category_info, text="Category Name:").grid(row=0, column=0, sticky=W, padx=4)
        self.category_name_entry.grid(row=0, column=1)

        Button(controls, text="Add Group", command=self.add_group).pack(side="left")
        Button(controls, text="Create Category", command=self.create_category).pack(side="left", padx=10)

    def refresh_layout(self):
        """Repack groups based on their order in the list."""
        for g in self.groups:
            g["frame"].pack_forget()

        for g in self.groups:
            g["frame"].pack(fill="x", pady=5, padx=5)

    def move_up(self, group):
        index = self.groups.index(group)
        if index > 0:
            self.groups[index], self.groups[index - 1] = self.groups[index - 1], self.groups[index]
            self.refresh_layout()

    def move_down(self, group):
        index = self.groups.index(group)
        if index < len(self.groups) - 1:
            self.groups[index], self.groups[index + 1] = self.groups[index + 1], self.groups[index]
            self.refresh_layout()

    def delete_group(self, group):
        group["frame"].destroy()
        self.groups.remove(group)

    def add_group(self):
        frame = Frame(self.scroll_frame, bd=2, relief="groove", padx=5, pady=5)

        name_entry = Entry(frame)
        type_var = StringVar(value=self.options[0])
        type_entry = OptionMenu(frame, type_var,*self.options)
        primary_key_var = BooleanVar(frame, value=False)
        primary_key = Checkbutton(frame, variable=primary_key_var)
        incrementer_var = BooleanVar(frame, value=False)
        incrementer = Checkbutton(frame, variable=incrementer_var)
        decimal_digits = NumberEntry(frame)


        Label(frame, text="Column Name:").grid(row=0, column=0, sticky=W, padx=4)
        name_entry.grid(row=0, column=1)

        Label(frame, text="Column Type:").grid(row=1, column=0, sticky=W)
        type_entry.grid(row=1, column=1, sticky=W)

        Label(frame, text="Primary Key:").grid(row=2, column=0, sticky=W)
        primary_key.grid(row=2, column=1, sticky=W)

        Label(frame, text="Incrementer:").grid(row=3, column=0, sticky=W)
        incrementer.grid(row=3, column=1, sticky=W)

        Label(frame, text="Decimal Digits:").grid(row=4, column=0, sticky=W)
        decimal_digits.grid(row=4, column=1, sticky=W)

        group = {
            "frame": frame,
            "name": name_entry,
            "type": type_var,
            "is_key": primary_key_var,
            "incrementer": incrementer_var,
            "decimal_digits": decimal_digits,
        }

        Button(frame, text="↑", command=lambda: self.move_up(group)).grid(row=0, column=2, padx=10)
        Button(frame, text="↓", command=lambda: self.move_down(group)).grid(row=4, column=2, padx=10)

        Button(frame, text="Delete", command=lambda: self.delete_group(group)).grid(row=4, column=3, padx=10)

        self.groups.append(group)

        self.refresh_layout()

    def create_category(self):
        has_primary_key = False
        category = Category()
        category.display_name = self.category_name_entry.get().strip()
        category.db_name = db_name(category.display_name)
        for i, g in enumerate(self.groups):
            name: str = g["name"].get_string()
            col_type: str = g["type"].get_string()
            col_prim: bool = g["is_key"].get_string()
            if col_prim:
                has_primary_key = True
            try:
                decimal_digits = int(g["decimal_digits"].get_string()) if col_type == "INTEGER" else 0
            except ValueError:
                decimal_digits = 0
            incrementer = col_type == "INTEGER" and decimal_digits == 0 and g["incrementer"].get_string()
            if db_name(name) in category.columns:
                # Todo: some kind of error popover
                return
            category.columns[db_name(name)] = (name, col_prim, col_type, decimal_digits, incrementer)
        if not has_primary_key:
            # Todo: some kind of error popover
            return
        if not category.add_category():
            # Todo: some kind of error popover
            return
        self.base.categories.append(category)

class EntryAdder:
    def __init__(self, category: Category):
        self.master = Toplevel()
        self.master.title("Create Entry into " + category.display_name)
        self.master.minsize(550, 400)
        self.master.focus_force()

        self.db_name = category.db_name
        self.col_names = ", ".join(list(category.columns.keys()))
        self.question_marks = ", ".join(["?"] * len(category.columns.keys()))
        self.elements = []
        self.default_values = []
        pos = 0
        for tab_name, col in category.columns.items():
            disp_name, _, col_type, decimal_digits, _ = col

            if col_type == "TEXT":
                var = StringVar(self.master)
                self.default_values.append("")
                self.elements.append(var)
                entry = Entry(self.master, textvariable=var)
            elif col_type == "BOOLEAN":
                var = BooleanVar(self.master)
                self.default_values.append(False)
                self.elements.append(var)
                entry = Checkbutton(self.master, variable=var)
            else:
                entry = NumberEntry(self.master, decimal_digits,None)
                self.default_values.append("")
                self.elements.append(entry)
            Label(self.master, text=disp_name+":").grid(row=pos, column=0, sticky=W, padx=4, pady=3)
            entry.grid(row=pos, column=1, sticky=W)
            pos += 1

        Button(self.master, text='Add Entry', command=self.add_to_db).grid(row=pos, column=0, pady=5)
        Button(self.master, text='Reset Entries', command=self.reset).grid(row=pos, column=1)

    def add_to_db(self):
        values = [e.get() for e in self.elements]
        try:
            with con as cur:
                command = f"INSERT INTO {self.db_name} ({self.col_names}) VALUES ({self.question_marks});"
                cur.execute(command, values)
        except sqlite3.Error:
            # Todo: some kind of error popover
            return
        self.reset()

    def reset(self):
        for i, element in enumerate(self.elements):
            element.set(self.default_values[i])

class TableView:
    def __init__(self, category: Category):
        self.category = category
        self.master = Toplevel()
        self.master.title("View " + category.display_name)
        self.master.minsize(550, 400)
        self.master.focus_force()

        category_db_name = category.db_name
        col_names = ", ".join(list(category.columns.keys()))
        cur = con.cursor()
        result = cur.execute(f"SELECT {col_names} FROM {category_db_name} ORDER BY {col_names};").fetchall()
        if result is None:
            self.master.destroy()

        t = SimpleTable(self.master, 10,2)
        t.pack(side="top", fill="x")
        t.set(0,0,"Hello, world this is extra long, show me what you got")

    @staticmethod
    def autosize_columns(tree):
        font = tk_font.nametofont("TkDefaultFont")

        for col in tree["columns"]:
            max_width = font.measure(col)

            for item in tree.get_children():
                text = str(tree.set(item, col))
                max_width = max(max_width, font.measure(text))

            tree.column(col, width=max_width + 20)



class BaseInterface:
    def __init__(self):
        self.master = Tk()
        self.master.title("Main Menu")
        self.master.minsize(550, 400)

        default_font = tk_font.nametofont("TkDefaultFont")
        default_font.configure(size=14)
        self.master.option_add("*Font", default_font)

        self.categories = Category.load_categories_from_db()
        self.interface()

        self.master.mainloop()

    def interface(self):
        def create_category() -> None:
            CategoryCreator(self)

        TableView(self.categories[0])

        Button(self.master, text='Create New Category', command=create_category).grid(row=0, column=0)


def main():
    BaseInterface()


if __name__ == '__main__':
    main()