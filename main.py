from __future__ import annotations

import atexit
import re
import sqlite3
from tkinter import *
import tkinter.font as tk_font
import tkinter.messagebox
from typing import Final, Callable, Any
from tkinter.scrolledtext import ScrolledText as TKScrollText
from functools import partial

#------------------------------------
# helper functions
#------------------------------------


def format_number(value: int | None, decimal_digits: int) -> str:
    if value is None:
        return ""
    if decimal_digits < 1:
        return str(value)
    base_string = str(value)
    base_string = ((decimal_digits - len(base_string) + 1) * "0") + base_string
    return base_string[:-decimal_digits] + "." + base_string[-decimal_digits:]

def format_number_trim(value: int | None, decimal_digits: int) -> str:
    if value is None:
        return ""
    if decimal_digits < 1:
        return str(value)
    base_string = format_number(value, decimal_digits)
    base_string = base_string.rstrip("0")
    return base_string.rstrip(".")


#------------------------------------
# DB Connection
#------------------------------------


find_spaces = re.compile(r"[ \t\n]", flags=re.MULTILINE | re.UNICODE)
find_non_letters = re.compile(r"[^a-zA-Z]", flags=re.MULTILINE | re.UNICODE)


def db_name(display_name: str) -> str:
    display_name = find_spaces.sub("_", display_name)
    return find_non_letters.sub("", display_name)


sqlite3.register_adapter(bool, int)
sqlite3.register_converter("BOOLEAN", lambda v: bool(int(v)))
con = sqlite3.connect(":memory:", detect_types=sqlite3.PARSE_DECLTYPES)
database_on_disk = sqlite3.connect("./data.sqlite", detect_types=sqlite3.PARSE_DECLTYPES)
with database_on_disk:
    database_on_disk.backup(con)

def update_disk_db(close: bool = True):
    with con:
        con.backup(database_on_disk)
    if close:
        con.close()
        database_on_disk.close()

atexit.register(update_disk_db)


#------------------------------------
# DB Abstractions
#------------------------------------


class TableTypes:
    STRING: Final[str] = "TEXT"
    INTEGER: Final[str] = "INTEGER"
    BOOLEAN: Final[str] = "BOOLEAN"

class Category:
    def __init__(self):
        self.id: int = -1
        self.db_name: str = ""
        self.display_name: str = ""
        self.column_text: str = ""
        self.__primary_key_statement: str = ""
        self.__base_full_update: str = ""
        self.__col_sql_list: str = ""
        self.primary_key_pos: list[int] = []
        self.__local_stored_result: list[list[Any]] = []
        self.incrementer_list: list[bool] = []
        self.col_display_names: list[str] = []

        # DB Col-Name to (Display Name, Primary key, Type, Decimal Digits, Incrementer, TextArea)
        self.columns: dict[str, tuple[str, bool, str, int, bool, bool]] = {}

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
        # helpful values that might be used internally or externally
        prim_key = [(key, pos) for pos, (key, (_, val, *_)) in enumerate(self.columns.items()) if val]
        self.primary_key_pos = [pos for _, pos in prim_key]
        self.__primary_key_statement = " AND ".join([f"{key} = ?" for key, _ in prim_key])
        self.__base_full_update = ", ".join([f"{key} = ?" for key in self.columns.keys()])
        self.__col_sql_list = ", ".join(list(self.columns.keys()))
        self.incrementer_list = [val for _, _, _, _, val, *_ in self.columns.values()]
        self.col_display_names = [val for val, *_ in self.columns.values()]

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
                      ", ".join([f"{key} {val}" for key, (_, _, val, *_) in self.columns.items()]) + ", PRIMARY KEY ("
        for key, val in self.columns.items():
            _, prim, *_ = val
            if prim:
                tab_creator += key + ", "
        return tab_creator.removesuffix(", ") + "));"

    def do_full_update(self, old_row_data: list, new_row_data: list) -> bool:
        command = f"UPDATE {self.db_name} SET {self.__base_full_update} WHERE {self.__primary_key_statement};"
        values = new_row_data + [old_row_data[i] for i in self.primary_key_pos]
        try:
            with con as local_cur:
                local_cur.execute(command, values)
        except sqlite3.Error as err:
            show_error(f"Could not update row:\n{err}")
            return False
        return True

    def delete_entry(self, data: list) -> bool:
        command = f"DELETE FROM {self.db_name} WHERE {self.__primary_key_statement};"
        values = [data[i] for i in self.primary_key_pos]
        try:
            with con as local_cur:
                local_cur.execute(command, values)
        except sqlite3.Error as err:
            show_error(f"Could not delete row:\n{err}")
            return False
        return True

    def do_partial_update(self, old_row_data: list, new_value, position: int) -> bool:
        command = f"UPDATE {self.db_name} SET {list(self.columns.keys())[position]} = ? WHERE " \
                  f"{self.__primary_key_statement};"
        values = [new_value] + [old_row_data[i] for i in self.primary_key_pos]
        try:
            with con as local_cur:
                local_cur.execute(command, values)
        except sqlite3.Error as err:
            show_error(f"Could not update field:\n{err}")
            return False
        return True

    def add_entry(self, data: list) -> bool:
        col_names = ", ".join(list(self.columns.keys()))
        question_marks = ", ".join(["?"] * len(self.columns.keys()))
        try:
            with con as cur:
                command = f"INSERT INTO {self.db_name} ({col_names}) VALUES ({question_marks});"
                cur.execute(command, data)
        except sqlite3.Error as e:
            show_error(f"Failed to insert new entry:\n{e}")
            return False
        return True

    def load_entry(self, data: list) -> list | None:
        cur = con.cursor()
        command = f"SELECT {self.__col_sql_list} FROM {self.db_name} WHERE {self.__primary_key_statement};"
        values = [data[i] for i in self.primary_key_pos]
        query_result = cur.execute(command, values).fetchone()
        cur.close()
        if query_result is None:
            return query_result
        return list(query_result)

    def query_full_table(self) -> list[list] | None:
        cur = con.cursor()
        command = f"SELECT {self.__col_sql_list} FROM {self.db_name} ORDER BY {self.__col_sql_list};"
        query_result = cur.execute(command).fetchall()
        cur.close()
        if query_result is None:
            show_info("Table Query failed")
            return None
        self.__local_stored_result = [list(e) for e in query_result]
        return self.__local_stored_result

    def transform_last_full_query_into_string(self) -> list[list[str]]:
        return [self._transform_values(e) for e in self.__local_stored_result]

    def _transform_values(self, data: list) -> list[str]:
        temp_col = list(self.columns.values())
        res: list[str] = []

        for i, val in enumerate(data):
            _, _, col_type, dec_digit, *_ = temp_col[i]

            if col_type == TableTypes.STRING:
                res.append(val)
            elif col_type == TableTypes.BOOLEAN:
                if val:
                    res.append("✅")
                else:
                    res.append("❌")
            elif col_type == TableTypes.INTEGER:
                res.append(format_number_trim(val, dec_digit))

        return res


#------------------------------------
# tkinter addition classes and functions
#------------------------------------


def show_error(msg: str):
    tkinter.messagebox.showerror("Error", msg)


def show_info(msg: str):
    tkinter.messagebox.showinfo("Info", msg)


class ScrollText:
    def __init__(self, text: TKScrollText):
        self.text = text

    def get(self) -> str:
        return self.text.get("1.0", END)

    def set(self, value: str):
        self.text.replace("1.0", END, value)


class ScrollableFrame(Frame):
    def __init__(self, parent, max_height=400, *args, **kwargs):
        super().__init__(parent, *args, **kwargs)

        self.canvas = Canvas(self, highlightthickness=0, height=max_height)
        self.scrollbar = Scrollbar(self, orient="vertical", command=self.canvas.yview)

        self.inner = Frame(self.canvas)

        self.window_id = self.canvas.create_window(
            (0, 0),
            window=self.inner,
            anchor="nw"
        )

        self.canvas.configure(yscrollcommand=self.scrollbar.set)

        self.canvas.pack(side="left", fill="both", expand=True)
        self.scrollbar.pack(side="right", fill="y")

        # Update scrollregion when content changes
        self.inner.bind("<Configure>", self._on_frame_configure)

        # Make inner frame width follow canvas width
        self.canvas.bind("<Configure>", self._on_canvas_configure)

        # Mousewheel scrolling
        self.canvas.bind_all("<MouseWheel>", self._on_mousewheel)
        self.canvas.bind_all("<Button-4>", self._on_mousewheel_linux_up)
        self.canvas.bind_all("<Button-5>", self._on_mousewheel_linux_down)

    def _on_frame_configure(self, _=None):
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))

    def _on_canvas_configure(self, event):
        self.canvas.itemconfig(self.window_id, width=event.width)

    def _on_mousewheel(self, event):
        self.canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

    def _on_mousewheel_linux_up(self, _):
        self.canvas.yview_scroll(-1, "units")

    def _on_mousewheel_linux_down(self, _):
        self.canvas.yview_scroll(1, "units")


class SimpleTable(Frame):
    def __init__(self, parent, data: list[list[str]], incrementer: list[bool],
                 func: Callable[[int, int, bool, tkinter.Label], None], **kwargs):
        # use black background so it "peeks through" to
        # form grid lines
        Frame.__init__(self, parent, background="black", **kwargs)
        self._widgets = []
        for row in range(len(data)):
            current_row = []
            for column in range(len(incrementer)):
                if incrementer[column] and row != 0:
                    sub_frame = Frame(self, background="lightblue" if row == 0 else
                                            ("white" if row % 2 == 1 else "light gray"), width="20")
                    label = Label(sub_frame, text=data[row][column],
                                  borderwidth=0,
                                  bg="lightblue" if row == 0 else ("white" if row % 2 == 1 else "light gray"),
                                  wraplength="250", justify="left")
                    label.grid(row=0, column=1, rowspan=2, padx=1)
                    loc_row = row - 1
                    loc_col = column
                    Button(sub_frame, text="↑",
                           command=partial(func,loc_row, loc_col, True, label)) \
                        .grid(row=0, column=0)
                    Button(sub_frame, text="↓",
                           command=partial(func, loc_row, loc_col, False, label)) \
                        .grid(row=1, column=0)
                    sub_frame.grid_columnconfigure(0, weight=0, minsize=30)
                    sub_frame.grid_columnconfigure(1, weight=1, minsize=250)
                    sub_frame.grid(row=row, column=column, sticky="nsew", padx=1, pady=1)
                else:
                    label = Label(self, text=data[row][column],
                                  borderwidth=0,
                                  bg="lightblue" if row == 0 else ("white" if row % 2 == 1 else "light gray"),
                                  wraplength="250", justify="left")
                    label.grid(row=row, column=column, sticky="nsew", padx=1, pady=1)
                    current_row.append(label)
            self._widgets.append(current_row)

        for i, column in enumerate(incrementer):
            self.grid_columnconfigure(i, weight=1, minsize=280 if column else 250)

    def set(self, row, column, value) -> None:
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
        self.get_string, self.set_string = self.var.get, self.var.set

    def check(self, *_):
        if re.search(self.format, self.get_string(), flags=re.MULTILINE | re.UNICODE):
            self.old_value = self.get_string()
        else:
            # there's non-digit characters in the input; reject this
            self.set_string(self.old_value)

    def set(self, value:int | None):
        self.old_value = format_number_trim(value, self.decimal_digits)
        self.set_string(self.old_value)

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
        back += "0" * (self.decimal_digits - len(back))
        return int(front + back)


#------------------------------------
# Tkinter Visual Interfaces
#------------------------------------


class CategoryCreator:
    def __init__(self, base:BaseInterface):
        self.base = base
        base.master.withdraw()
        self.options = [TableTypes.STRING, TableTypes.INTEGER, TableTypes.BOOLEAN]

        self.master = Toplevel()
        self.master.protocol("WM_DELETE_WINDOW", self.destroy)
        self.master.title("Create Category")
        self.master.minsize(550, 400)
        self.master.focus_force()

        outer = ScrollableFrame(self.master)
        self.scroll_frame = outer.inner

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

    def destroy(self):
        self.base.master.deiconify()
        self.master.destroy()

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
        text_area_var = BooleanVar(frame, value=False)
        text_area = Checkbutton(frame, variable=text_area_var)
        incrementer_var = BooleanVar(frame, value=False)
        incrementer = Checkbutton(frame, variable=incrementer_var)
        decimal_digits = NumberEntry(frame)


        Label(frame, text="Column Name:").grid(row=0, column=0, sticky=W, padx=4)
        name_entry.grid(row=0, column=1)

        Label(frame, text="Column Type:").grid(row=1, column=0, sticky=W)
        type_entry.grid(row=1, column=1, sticky=W)

        Label(frame, text="Primary Key:").grid(row=2, column=0, sticky=W)
        primary_key.grid(row=2, column=1, sticky=W)

        Label(frame, text="Enable Textarea:").grid(row=3, column=0, sticky=W)
        text_area.grid(row=3, column=1, sticky=W)

        Label(frame, text="Incrementer:").grid(row=4, column=0, sticky=W)
        incrementer.grid(row=4, column=1, sticky=W)

        Label(frame, text="Decimal Digits:").grid(row=5, column=0, sticky=W)
        decimal_digits.grid(row=5, column=1, sticky=W)

        group = {
            "frame": frame,
            "name": name_entry,
            "type": type_var,
            "is_key": primary_key_var,
            "text_area": text_area_var,
            "incrementer": incrementer_var,
            "decimal_digits": decimal_digits,
        }

        Button(frame, text="↑", command=lambda: self.move_up(group)).grid(row=0, column=2, padx=10)
        Button(frame, text="↓", command=lambda: self.move_down(group)).grid(row=5, column=2, padx=10)

        Button(frame, text="Delete", command=lambda: self.delete_group(group)).grid(row=5, column=3, padx=10)

        self.groups.append(group)

        self.refresh_layout()

    def create_category(self):
        has_primary_key = False
        category = Category()
        category.display_name = self.category_name_entry.get().strip()
        category.db_name = db_name(category.display_name)
        for i, g in enumerate(self.groups):
            name: str = g["name"].get()
            col_type: str = g["type"].get()
            col_prim: bool = g["is_key"].get()
            text_area: bool = g["text_area"].get() and col_type == TableTypes.STRING
            if col_prim:
                has_primary_key = True
            try:
                decimal_digits = int(g["decimal_digits"].get()) if col_type == TableTypes.INTEGER else 0
            except ValueError:
                decimal_digits = 0
            incrementer = col_type == TableTypes.INTEGER and decimal_digits == 0 and g["incrementer"].get()
            if db_name(name) in category.columns:
                show_error(f"Field '{name}' would create a duplicate column name in database")
                return
            category.columns[db_name(name)] = (name, col_prim, col_type, decimal_digits, incrementer, text_area)
        if not has_primary_key:
            show_error("Category can't be added\nMissing at least one primary key")
            return
        if not category.add_category():
            show_error("Failed to add Category")
            return
        self.base.categories.append(category)
        show_info("Successfully added Category")

class EntryManipulator:
    def __init__(self, base: BaseInterface, category: Category):
        self.base = base
        self.category = category
        base.master.withdraw()

        self.master = Toplevel()
        self.master.protocol("WM_DELETE_WINDOW", self.destroy)
        self.master.title("Create Entry into " + category.display_name)
        self.master.minsize(550, 400)
        self.master.focus_force()

        self.dropdown_text = StringVar(self.master, "Add Entry")
        self.dropdown_text.trace_add("write", self.on_update)
        self.mode_dict = {
            "Add Entry": lambda : self.set_button_for_add_entry(),
            "Manipulate Entry": lambda: self.set_button_for_modify_entry(),
            "Delete Entry": lambda: self.set_button_for_delete_entry()
        }
        Label(self.master, text="Edit Mode:").grid(row=0, column=0, sticky=W, padx=4)
        dropdown = OptionMenu(self.master, self.dropdown_text,
                              *list(self.mode_dict.keys()))
        dropdown.grid(column=1, row=0, pady=5, sticky="w")

        self.var_elements = []
        self.elements = []
        self.default_values = []
        self.queried_entry = []

        pos = 1
        for tab_name, col in category.columns.items():
            disp_name, _, col_type, decimal_digits, _, text_area = col

            if col_type == TableTypes.STRING and text_area:
                entry = TKScrollText(self.master, wrap=WORD, height=3, width=30)
                self.default_values.append("")
                self.var_elements.append(ScrollText(entry))
            elif col_type == TableTypes.STRING:
                var = StringVar(self.master)
                self.default_values.append("")
                self.var_elements.append(var)
                entry = Entry(self.master, textvariable=var, width=30)
            elif col_type == TableTypes.BOOLEAN:
                var = BooleanVar(self.master)
                self.default_values.append(False)
                self.var_elements.append(var)
                entry = Checkbutton(self.master, variable=var)
            elif col_type == TableTypes.INTEGER:
                entry = NumberEntry(self.master, decimal_digits,None, width=15)
                self.default_values.append(None)
                self.var_elements.append(entry)
            else:
                continue
            Label(self.master, text=disp_name+":").grid(row=pos, column=0, sticky=W, padx=4, pady=3)
            entry.grid(row=pos, column=1, sticky=W)
            self.elements.append(entry)
            pos += 1

        frame = Frame(self.master)
        frame.grid(row=pos, column=0, columnspan=2, pady=5)
        self.btn_one = Button(frame)
        self.btn_one.grid(row=pos, column=0)
        self.btn_two = Button(frame)
        self.btn_two.grid(row=pos, column=1, padx=5, sticky="w")
        self.btn_three = Button(frame)
        self.btn_three.grid(row=pos, column=2, padx=5, sticky="w")
        self.set_button_for_add_entry()

    def on_update(self, *_):
        self.mode_dict[self.dropdown_text.get()]()

    def set_button_for_add_entry(self):
        self.btn_one.config(text='Add Entry', command=self.add_to_db)
        self.btn_two.config(text='Reset Fields', command=self.reset)
        self.btn_three.config(text='Free Entry', command=lambda: None, state="disabled")

    def set_button_for_modify_entry(self):
        self.btn_one.config(text='Query Entry', command=self.query_entries)
        self.btn_two.config(text='Update Entry', command=self.modify_entry, state="disabled")
        self.btn_three.config(text='Free Entry', command=self.free_entry, state="disabled")

    def set_button_for_delete_entry(self):
        self.btn_one.config(text='Query Entry', command=lambda: self.query_entries(True))
        self.btn_two.config(text='Delete Queried Entry', command=self.delete_entry, state="disabled")
        self.btn_three.config(text='Free Entry', command=self.free_entry, state="disabled")

    def query_entries(self, lock_fields: bool = False):
        values = [e.get() for e in self.var_elements]
        values = [e if type(e) is not str else e.strip() for e in values]
        result = self.category.load_entry(values)
        if result is None:
            show_info("Could not find an entry with the given key")
            return
        self.queried_entry = result.copy()
        self.btn_one.configure(state="disabled")
        self.btn_two.configure(state="active")
        self.btn_three.configure(state="active")
        for i, element in enumerate(self.var_elements):
            element.set(self.queried_entry[i])
        if lock_fields:
            self.disable_entries()

    def modify_entry(self):
        values = [e.get() for e in self.var_elements]
        values = [e if type(e) is not str else e.strip() for e in values]
        if not self.category.do_full_update(self.queried_entry, values):
            return
        self.btn_one.configure(state="active")
        self.btn_two.configure(state="disabled")
        self.btn_three.configure(state="disabled")
        self.reset()

    def delete_entry(self):
        if not self.category.delete_entry(self.queried_entry):
            return
        self.enable_entries()
        self.reset()
        self.btn_one.configure(state="active")
        self.btn_two.configure(state="disabled")
        self.btn_three.configure(state="disabled")

    def free_entry(self):
        self.enable_entries()
        self.btn_one.configure(state="active")
        self.btn_two.configure(state="disabled")
        self.btn_three.configure(state="disabled")
        self.reset()

    def disable_entries(self):
        [entry.config(state="disabled") for entry in self.elements]

    def enable_entries(self):
        [entry.config(state="normal") for entry in self.elements]

    def destroy(self):
        self.base.master.deiconify()
        self.master.destroy()

    def add_to_db(self):
        values = [e.get() for e in self.var_elements]
        values = [e if type(e) is not str else e.strip() for e in values]
        if not self.category.add_entry(values):
            return
        self.reset()

    def reset(self):
        for i, element in enumerate(self.var_elements):
            element.set(self.default_values[i])


class TableView:
    def __init__(self, category: Category):
        self.category = category
        self.master = Toplevel()
        self.master.title("View " + category.display_name)
        self.master.minsize(550, 400)
        self.master.focus_force()

        query_result: list[list[Any]] = category.query_full_table()
        if query_result is None:
            self.master.destroy()

        result: list[list[str]] = category.transform_last_full_query_into_string()


        def update_label(row: int, col_pos: int, increment: bool,
                         widget_to_update: tkinter.Label) -> None:
            column = query_result[row]
            col_copy = query_result[row].copy()
            if col_copy[col_pos] is None:
                col_copy[col_pos] = 1 if increment else 0
            else:
                col_copy[col_pos] += 1 if increment else (-1 if col_copy[col_pos] > 0 else 0)
            if not category.do_partial_update(column, col_copy[col_pos], col_pos):
                return
            widget_to_update.config(text=str(col_copy[col_pos]))
            query_result[row] = col_copy

        scroll = ScrollableFrame(self.master, max_height=300)
        scroll.pack(fill="both", expand=True, padx=10, pady=10)

        frame = SimpleTable(scroll.inner, [self.category.col_display_names] + result,
                            category.incrementer_list, update_label)
        frame.pack(side="top", fill="x")
        self.master.update_idletasks()
        h = frame.winfo_height()
        self.master.geometry(f"{frame.winfo_width() + 40}x{h if h < 1000 else 1000}")


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

        EntryManipulator(self, self.categories[1])
        #TableView(self.categories[1])

        Button(self.master, text='Create New Category', command=create_category).grid(row=0, column=0)


def main():
    BaseInterface()


if __name__ == '__main__':
    main()