from __future__ import annotations

import atexit
import csv
import html
import io
import json
import re
import sqlite3
import xml.etree.ElementTree as ElTree
import zipfile
from tkinter import *
import tkinter.font as tk_font
import tkinter.messagebox
from typing import Final, Callable, Any
from tkinter.scrolledtext import ScrolledText as TKScrollText
from tkinter.filedialog import askopenfilename
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


def format_string_to_number(value: str, decimal_digits: int) -> int | None:
    value = value.strip()
    general_format = re.compile(r"^[0-9]+(\.[0-9]*)?$", flags=re.MULTILINE | re.UNICODE)
    num_format = re.compile(r"^[0-9]*(\.[0-9]{0," + str(decimal_digits) + r"})?" if decimal_digits > 0 else r"^[0-9]*",
                        flags=re.MULTILINE | re.UNICODE)
    if general_format.search(value):
        m = num_format.search(value).group()
        if decimal_digits < 1:
            return int(m)
        if m.count(".") > 0 and not m.endswith("."):
            amt = len(m.split(".")[-1])
            m = int(m.replace(".", ""))
            return m * (10 ** (decimal_digits - amt))
        else:
            return int(m.removesuffix(".")) * (10 ** decimal_digits)
    else:
        return None


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
con.isolation_level = None
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


def open_cursor() -> sqlite3.Cursor:
    cur = con.cursor()
    cur.execute("BEGIN")
    return cur


def rollback(cur: sqlite3.Cursor, error_msg:str = None):
    cur.execute("ROLLBACK")
    cur.close()
    if error_msg is not None:
        show_error(error_msg)


def commit(cur: sqlite3.Cursor):
    cur.execute("COMMIT")
    cur.close()


#------------------------------------
# DB Abstractions
#------------------------------------


class TableTypes:
    STRING: Final[str] = "TEXT"
    INTEGER: Final[str] = "INTEGER"
    BOOLEAN: Final[str] = "BOOLEAN"


class CategoryColumn:
    __COL_NAME: Final[str] = "columnName"
    __DISPLAY_NAME: Final[str] = "displayName"
    __IS_PRIMARY_KEY: Final[str] = "isPrimaryKey"
    __COL_TYPE: Final[str] = "columnType"
    __DECIMAL_DIGITS: Final[str] = "decimalDigits"
    __INCREMENTER: Final[str] = "incrementer"
    __TEXT_AREA: Final[str] = "textarea"

    def __init__(self, col_name: str | None, display_name: str | None, is_primary_key: bool | None,
                 col_type: str | None, decimal_digits: int | None, incrementer: bool | None, text_area: bool | None):
        self.col_name: str = col_name if col_name is not None else ""
        self.display_name: str = display_name if display_name is not None else ""
        self.is_primary_key: bool = is_primary_key if is_primary_key is not None else False
        self.col_type: str = col_type if col_type is not None else TableTypes.STRING
        self.decimal_digits: int = decimal_digits if decimal_digits is not None else 0
        self.incrementer: bool = incrementer if incrementer is not None else False
        self.text_area: bool = text_area if text_area is not None else False

    @classmethod
    def load_columns_from_dict(cls, data: dict[str, dict[str, Any]]) -> tuple[list[CategoryColumn], bool]:
        columns = []
        update_necessary = False

        for key in data.keys():
            col_dict = data[key]
            values: list[Any] = [key]
            for temp_key in [cls.__DISPLAY_NAME, cls.__IS_PRIMARY_KEY, cls.__COL_TYPE, cls.__DECIMAL_DIGITS,
                             cls.__INCREMENTER, cls.__TEXT_AREA]:
                if temp_key in col_dict:
                    values.append(col_dict[temp_key])
                else:
                    update_necessary = True
                    values.append(None)

            columns.append(CategoryColumn(*values))

        return columns, update_necessary

    @classmethod
    def transform_columns_to_dict(cls, columns: list[CategoryColumn]) -> dict[str, dict[str, Any]]:
        return {e.col_name: {
            cls.__DISPLAY_NAME: e.display_name,
            cls.__IS_PRIMARY_KEY: e.is_primary_key,
            cls.__COL_TYPE: e.col_type,
            cls.__DECIMAL_DIGITS: e.decimal_digits,
            cls.__INCREMENTER: e.incrementer,
            cls.__TEXT_AREA: e.text_area
        } for e in columns}

    def transform_string_for_search(self, in_str: str) -> str | bool | int | None | tuple[str, int | None]:
        if self.col_type == TableTypes.STRING:
            return in_str.strip()
        elif self.col_type == TableTypes.BOOLEAN:
            return in_str.strip().upper() in ["1", "YES", "TRUE"]
        elif self.col_type == TableTypes.INTEGER:
            if any([in_str.startswith(val) for val in ["= ", "== ", "> ", "< ", "<= ", ">= ", "!= ", "<> "]]):
                extra = in_str[:2].strip()
                return extra, format_string_to_number(in_str[2:], self.decimal_digits)
            return format_string_to_number(in_str, self.decimal_digits)
        return None

    def transform_personal_value_to_string(self, val: Any) -> str:
        if self.col_type == TableTypes.STRING:
            return val
        elif self.col_type == TableTypes.BOOLEAN:
            if val:
                return "✅"
            else:
                return "❌"
        elif self.col_type == TableTypes.INTEGER:
            return format_number_trim(val, self.decimal_digits)
        return ""


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
        self.incrementer_list: list[bool] = []
        self.col_display_names: list[str] = []
        self.db_column_names: list[str] = []
        self.db_primary_columns: list[str] = []

        # DB Col-Name to (Display Name, Primary key, Type, Decimal Digits, Incrementer, TextArea)
        self.columns: list[CategoryColumn] = []

    @classmethod
    def load_categories_from_db(cls) -> list[Category]:
        cur = open_cursor()
        res_query = cur.execute("SELECT COUNT(*) AS nums FROM sqlite_master WHERE type='table' AND name='master';").fetchone()
        if not res_query or res_query[0] < 1:
            cur.execute("CREATE TABLE master (ID INTEGER PRIMARY KEY AUTOINCREMENT, DB_NAME TEXT UNIQUE, NAME TEXT, COLUMNS TEXT);")
            commit(cur)
            return []
        res_query = cur.execute("SELECT ID, DB_NAME, NAME, COLUMNS FROM master ORDER BY ID;").fetchall()
        commit(cur)
        if res_query is None:
            return []
        categories = []
        for entry in res_query:
            cat = Category()
            cat.id, cat.db_name, cat.display_name, cat.column_text = entry
            if cat._transform_text():
                cat._update_in_db()
            categories.append(cat)
        return categories

    def _transform_text(self) -> bool:
        if len(self.columns) == 0:
            self.columns, update = CategoryColumn.load_columns_from_dict(json.loads(self.column_text))
        else:
            self.column_text = json.dumps(CategoryColumn.transform_columns_to_dict(self.columns))
            update = False

        # helpful values that might be used internally or externally
        prim_key = [(col.col_name, pos) for pos, col in enumerate(self.columns) if col.is_primary_key]
        self.primary_key_pos = [pos for _, pos in prim_key]
        self.db_primary_columns = [key for key, _ in prim_key]
        self.__primary_key_statement = " AND ".join([f"{key} = ?" for key, _ in prim_key])
        self.__base_full_update = ", ".join([f"{col.col_name} = ?" for col in self.columns])
        self.__col_sql_list = ", ".join([col.col_name for col in self.columns])
        self.incrementer_list = [col.incrementer for col in self.columns]
        self.col_display_names = [col.display_name for col in self.columns]
        self.db_column_names = [col.col_name for col in self.columns]

        return update

    def get_default_row(self) -> list[Any]:
        result = []
        for c in self.columns:
            if TableTypes.STRING == c.col_type:
                result.append("")
            elif TableTypes.BOOLEAN == c.col_type:
                result.append(False)
            else:
                result.append(None)
        return result

    def column_position(self, disp_name: str | list[str], col_type: str,
                              decimal_digits: int | None = None) -> int:
        if isinstance(disp_name, list):
            disp_names = [e.upper() for e in disp_name]
        else:
            disp_names = [disp_name.upper()]
        for pos, col in enumerate(self.columns):
            if col.display_name.upper() in disp_names and col.col_type == col_type and \
                    (decimal_digits is None or decimal_digits == col.decimal_digits):
                return pos
        return -1

    def has_column_definition(self, disp_name: str | list[str], col_type: str,
                              decimal_digits: int | None = None) -> bool:
        return self.column_position(disp_name, col_type, decimal_digits) != -1


    def add_category(self) -> bool:
        self._transform_text()
        cur = open_cursor()
        try:
            cat_id = cur.execute("INSERT INTO master(DB_NAME, NAME, COLUMNS) VALUES(?, ?, ?) RETURNING ID",
                                 (self.db_name, self.display_name, self.column_text,)).fetchone()
            cur.execute(self._try_create_table())
            (self.id, ) = cat_id if cat_id else -1
            commit(cur)
        except sqlite3.Error as err:
            rollback(cur, f"Could not create Category '{self.display_name}':\n{err}")
            return False
        return True

    def _update_in_db(self) -> bool:
        if self.id == -1:
            return False
        self._transform_text()
        cur = open_cursor()
        try:
            cur.execute("UPDATE master SET DB_NAME = ?, NAME = ?, COLUMNS = ? WHERE ID = ?;",
                        (self.db_name, self.display_name, self.column_text, self.id))
            commit(cur)
        except sqlite3.Error:
            rollback(cur)
            return False
        return True

    def update_category(self, new_values: Category) -> bool:
        old_data = self.query_full_table("Could not retrieve data for migration")
        if old_data is None:
            return False
        old_data: list[list] = [list(e) for e in zip(*old_data)]
        new_data: list[list] = []
        cur = open_cursor()
        try:
            cur.execute(f"DROP TABLE {self.db_name};")
            for pos, col in enumerate(new_values.columns):
                new_column_value, new_column = self._generate_new_column(col, old_data)
                if new_column_value is not None:
                    new_data.append(new_column_value)
                new_values.columns[pos] = new_column

            self.display_name = new_values.display_name
            self.db_name = new_values.db_name
            self.columns = new_values.columns
            self._transform_text()
            cur.execute(self._try_create_table())
            cur.execute("UPDATE master SET DB_NAME = ?, NAME = ?, COLUMNS = ? WHERE ID = ?;",
                        (self.db_name, self.display_name, self.column_text, self.id))
            col_names = ", ".join(self.db_column_names)
            question_marks = ", ".join(["?"] * len(self.db_column_names))
            command = f"INSERT INTO {self.db_name} ({col_names}) VALUES ({question_marks});"
            new_data = [list(e) for e in zip(*new_data)]
            if len(new_data) > 0:
                cur.executemany(command, new_data)
            commit(cur)
        except Exception as err:
            rollback(cur, f"Failed to update table:\n{err}")
            return False
        return True

    def _generate_new_column(self, col: CategoryColumn, old_data: list[list]) -> tuple[list | None, CategoryColumn]:
        old_column_name = col.display_name.split("->")[0].strip()
        col.display_name = col.display_name.split("->")[-1].strip()
        if len(old_data) == 0:
            return None, col
        if old_column_name in self.col_display_names:
            prev_col_pos = self.col_display_names.index(old_column_name)
            data = old_data[prev_col_pos]
            new_data = self._transform_old_data(self.columns[prev_col_pos], col, data)
        else:
            row_amount = len(old_data[0])
            new_data = [self._get_type_default(col.col_type)] * row_amount
        return new_data, col

    @staticmethod
    def _transform_old_data(old_col: CategoryColumn, new_col: CategoryColumn, data: list) -> list:
        if old_col.col_type != new_col.col_type:
            if new_col.col_type == TableTypes.STRING:
                if old_col.col_type == TableTypes.BOOLEAN:
                    return ["✅" if val else "❌" for val in data]
                else:
                    # Has to be integer
                    return [format_number_trim(val, old_col.decimal_digits) for val in data]
            if new_col.col_type == TableTypes.INTEGER:
                if old_col.col_type == TableTypes.STRING:
                    return [format_string_to_number(val, new_col.decimal_digits) for val in data]
                else:
                    # Has to be boolean
                    power = 10 ** new_col.decimal_digits
                    return [power if val else 0 for val in data]
            else:
                # New col type is boolean
                return [True if val else False for val in data]


        if old_col.decimal_digits == new_col.decimal_digits:
            return data
        if old_col.decimal_digits > new_col.decimal_digits:
            power = (10 ** (old_col.decimal_digits - new_col.decimal_digits))
            return [val if val is None else val // power for val in data]
        power = (10 ** (new_col.decimal_digits - old_col.decimal_digits))
        return [val if val is None else val * power for val in data]

    @staticmethod
    def _get_type_default(col_type: str) -> Any:
        if TableTypes.STRING == col_type:
            return ""
        elif TableTypes.INTEGER == col_type:
            return None
        elif TableTypes.BOOLEAN == col_type:
            return False
        return None

    def delete(self) -> bool:
        cur = open_cursor()
        try:
            cur.execute("DELETE FROM master WHERE ID = ?;",
                        (self.id,))
            cur.execute(f"DROP TABLE {self.db_name};")
            commit(cur)
        except sqlite3.Error as err:
            rollback(cur, f"Could remove category '{self.display_name}':\n{err}")
            return False
        return True

    def _try_create_table(self):
        tab_creator = f"CREATE TABLE {self.db_name} (" + \
                      ", ".join([f"{col.col_name} {col.col_type}" for col in self.columns]) + ", PRIMARY KEY ("
        for col in self.columns:
            if col.is_primary_key:
                tab_creator += col.col_name + ", "
        return tab_creator.removesuffix(", ") + "));"

    def do_full_update(self, old_row_data: list, new_row_data: list) -> bool:
        command = f"UPDATE {self.db_name} SET {self.__base_full_update} WHERE {self.__primary_key_statement};"
        values = new_row_data + [old_row_data[i] for i in self.primary_key_pos]
        cur = open_cursor()
        try:
            cur.execute(command, values)
            commit(cur)
        except sqlite3.Error as err:
            rollback(cur, f"Could not update row:\n{err}")
            return False
        return True

    def delete_entry(self, data: list) -> bool:
        command = f"DELETE FROM {self.db_name} WHERE {self.__primary_key_statement};"
        values = [data[i] for i in self.primary_key_pos]
        cur = open_cursor()
        try:
            cur.execute(command, values)
            commit(cur)
        except sqlite3.Error as err:
            rollback(cur, f"Could not delete row:\n{err}")
            return False
        return True

    def do_partial_update(self, old_row_data: list, new_value, position: int) -> bool:
        command = f"UPDATE {self.db_name} SET {self.db_column_names[position]} = ? WHERE " \
                  f"{self.__primary_key_statement};"
        values = [new_value] + [old_row_data[i] for i in self.primary_key_pos]
        cur = open_cursor()
        try:
            cur.execute(command, values)
            commit(cur)
        except sqlite3.Error as err:
            rollback(cur, f"Could not update field:\n{err}")
            return False
        return True

    def add_entry(self, values: list) -> bool:
        col_names = ", ".join(self.db_column_names)
        question_marks = ", ".join(["?"] * len(self.db_column_names))
        command = f"INSERT INTO {self.db_name} ({col_names}) VALUES ({question_marks});"
        cur = open_cursor()
        try:
            cur.execute(command, values)
            commit(cur)
        except sqlite3.Error as err:
            rollback(cur, f"Failed to insert new entry:\n{err}")
            return False
        return True

    def upsert_entry(self, values: list) -> bool:
        col_names = ", ".join(self.db_column_names)
        question_marks = ", ".join(["?"] * len(self.db_column_names))
        primary_keys = ", ".join(self.db_primary_columns)
        values = values + values + [values[i] for i in self.primary_key_pos]
        command = f"INSERT INTO {self.db_name} ({col_names}) VALUES ({question_marks}) ON CONFLICT" + \
                  f"({primary_keys}) DO UPDATE SET {self.__base_full_update} WHERE {self.__primary_key_statement};"
        cur = open_cursor()
        try:
            cur.execute(command, values)
            commit(cur)
        except sqlite3.Error as err:
            rollback(cur, f"Failed to upsert entry:\n{err}")
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

    def query_full_table(self, error_text:str=None) -> list[list] | None:
        cur = con.cursor()
        command = f"SELECT {self.__col_sql_list} FROM {self.db_name} ORDER BY {self.__col_sql_list};"
        query_result = cur.execute(command).fetchall()
        cur.close()
        if query_result is None:
            if error_text is not None:
                show_error(error_text)
                return None
            show_info("Table Query failed")
            return None
        return [list(e) for e in query_result]

    def query_first_page(self, error_text:str=None, row_amount: int = 10, queries: list=None) \
            -> tuple[list[list] | None, bool]:
        cur = con.cursor()
        where_clause, values = self._build_dynamic_where(queries)
        command = f"SELECT {self.__col_sql_list} FROM {self.db_name} {where_clause} " \
                  f"ORDER BY {self.__col_sql_list} LIMIT {row_amount+1};"
        query_result = cur.execute(command, values).fetchall()
        cur.close()
        if query_result is None:
            if error_text is not None:
                show_error(error_text)
                return None, False
            show_info("Table Query failed")
            return None, False
        return [list(e) for e in query_result[:row_amount]], len(query_result) == row_amount + 1

    def query_all_other_pages(self, error_text:str=None, row_amount: int = 10, queries: list=None) \
            -> list[list[list]] | None:
        cur = con.cursor()
        where_clause, values = self._build_dynamic_where(queries)
        command = f"SELECT {self.__col_sql_list} FROM {self.db_name} {where_clause} " \
                  f"ORDER BY {self.__col_sql_list} LIMIT -1 OFFSET {row_amount};"
        query_result = cur.execute(command, values).fetchall()
        cur.close()
        if query_result is None:
            if error_text is not None:
                show_error(error_text)
                return None
            show_info("Table Query failed")
            return None
        pages = ((len(query_result) - 1) // row_amount) + 1
        result = [[list(e) for e in query_result[off*row_amount:(off+1)*row_amount]] for off in range(pages)]
        return result

    def transform_query_into_string(self, data: list[list[Any]]) -> list[list[str]]:
        return [self._transform_values(e) for e in data]

    def _build_dynamic_where(self, user_inputs: list[str] | None) -> tuple[str, list[Any]]:
        if user_inputs is None:
            return "", []
        conditions = []
        params = []

        for col, raw_val, transformer in zip(self.db_column_names, user_inputs, self.columns):
            if raw_val is None or raw_val.strip() == "":
                continue
            val = transformer.transform_string_for_search(raw_val)

            if isinstance(val, tuple):
                # Operator-based numeric search
                op, num = val
                if num is None:
                    continue
                conditions.append(f'{col} {op} ?')
                params.append(num)
            elif isinstance(val, (bool, int)):
                # Boolean / numeric exact search
                conditions.append(f'{col} = ?')
                params.append(val)
            elif isinstance(val, str):
                # String search using LIKE
                conditions.append(f"{col} LIKE ? ESCAPE '\\'")
                params.append(f"%{val}%")
            else:
                conditions.append(f"{col} IS NULL")

        if not conditions:
            return "", []

        where_clause = "WHERE " + " AND ".join(conditions)
        return where_clause, params

    def _transform_values(self, data: list) -> list[str]:
        return [col.transform_personal_value_to_string(data[i]) for i, col in enumerate(self.columns)]


#------------------------------------
# tkinter addition classes and functions
#------------------------------------


def show_error(msg: str):
    tkinter.messagebox.showerror("Error", msg)


def show_info(msg: str):
    tkinter.messagebox.showinfo("Info", msg)


class SelectableLabel(Frame):
    def __init__(self, parent, text="", **kwargs):
        super().__init__(parent)

        self.text = text

        self.label = Label(self, text=text, anchor="w", **kwargs)
        self.label.pack(fill="both", expand=True)

        self.entry = Entry(
            self,
            relief="flat",
            bg=self.label.cget("bg"),
            readonlybackground=self.label.cget("bg"),
            fg=self.label.cget("fg"),
            font=self.label.cget("font"),
            highlightthickness=0
        )

        self.label.bind("<Button-1>", self._activate_entry)
        self.entry.bind("<FocusOut>", self._deactivate_entry)
        self.entry.bind("<Return>", self._deactivate_entry)
        self.entry.bind("<Escape>", self._deactivate_entry)

    def _activate_entry(self, _=None):
        self.entry.delete(0, "end")
        self.entry.insert(0, self.text)
        self.entry.config(state="readonly")
        self.label.pack_forget()
        self.entry.pack(fill="both", expand=True)

        self.entry.focus_set()
        self.entry.select_range(0, "end")

    def _deactivate_entry(self, _=None):
        self.text = self.entry.get()

        self.entry.pack_forget()
        self.label.config(text=self.text)
        self.label.pack(fill="both", expand=True)

    def config(self, **kwargs):
        if "text" in kwargs:
            self.text = kwargs["text"]
            self.label.config(text=self.text)
            kwargs.pop("text")

        self.label.config(**kwargs)


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

        self.window_id = self.canvas.create_window((0, 0), window=self.inner, anchor="nw")

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

    def unbind_scroll(self):
        self.canvas.unbind_all("<MouseWheel>")
        self.canvas.unbind_all("<Button-4>")
        self.canvas.unbind_all("<Button-5>")

    def bind_scroll(self):
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
                 func: Callable[[int, int, bool, SelectableLabel], None], **kwargs):
        # use black background so it "peeks through" to
        # form grid lines
        self._func = func
        self._incrementer = incrementer
        self._amount_rows = len(data)
        self._width = len(incrementer)
        Frame.__init__(self, parent, background="black", **kwargs)
        self._label_widget = []
        self._row_widget = []
        for row in range(len(data)):
            self._add_row(row, data[row])

        for i, column in enumerate(incrementer):
            self.grid_columnconfigure(i, weight=1, minsize=280 if column else 250)

    @staticmethod
    def _row_color(row: int) -> str:
        return "lightblue" if row == 0 else ("white" if row % 2 == 1 else "light gray")

    def set(self, row: int, column: int, value: str) -> None:
        widget = self._label_widget[row][column]
        widget.config(text=value)

    def resize_table(self, new_amount_rows: int) -> None:
        if new_amount_rows == self._amount_rows:
            return

        if new_amount_rows > self._amount_rows:
            calc = new_amount_rows - self._amount_rows
            data = [""] * len(self._incrementer)
            for _ in range(calc):
                self._add_row(self._amount_rows, data)
                self._amount_rows += 1
        else:
            calc = self._amount_rows - new_amount_rows
            self._amount_rows = new_amount_rows
            for i in range(calc):
                [e.destroy() for e in self._row_widget[-1-i]]
            self._row_widget = self._row_widget[:self._amount_rows]
            self._label_widget = self._label_widget[:self._amount_rows]

    def _add_row(self, row: int, data: list) -> None:
        current_row = []
        current_row_main_element = []
        for column in range(len(self._incrementer)):
            if self._incrementer[column] and row != 0:
                sub_frame = Frame(self, background=self._row_color(row), width="20")
                label = SelectableLabel(sub_frame, text=data[column], borderwidth=0,
                                        bg=self._row_color(row), wraplength="250", justify="left")
                label.grid(row=0, column=1, rowspan=2, padx=1)
                loc_row = row - 1
                loc_col = column
                Button(sub_frame, text="↑",
                       command=partial(self._func, loc_row, loc_col, True, label)) \
                    .grid(row=0, column=0)
                Button(sub_frame, text="↓",
                       command=partial(self._func, loc_row, loc_col, False, label)) \
                    .grid(row=1, column=0)
                sub_frame.grid_columnconfigure(0, weight=0, minsize=30)
                sub_frame.grid_columnconfigure(1, weight=1, minsize=250)
                sub_frame.grid(row=row, column=column, sticky="nsew", padx=1, pady=1)
                current_row_main_element.append(sub_frame)
            else:
                label = SelectableLabel(self, text=data[column], borderwidth=0,
                                        bg=self._row_color(row), wraplength="250", justify="left")
                label.grid(row=row, column=column, sticky="nsew", padx=1, pady=1)
                current_row_main_element.append(label)
            current_row.append(label)
        self._row_widget.append(current_row_main_element)
        self._label_widget.append(current_row)


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


class CategoryEditor:
    def __init__(self, base:BaseInterface):
        self.base = base
        base.hide()
        self.options = [TableTypes.STRING, TableTypes.INTEGER, TableTypes.BOOLEAN]

        self.master = Toplevel()
        self.master.protocol("WM_DELETE_WINDOW", self.destroy)
        self.master.title("Create Category")
        self.master.minsize(550, 400)
        self.master.focus_force()

        self.outer = ScrollableFrame(self.master)
        self.scroll_frame = self.outer.inner

        self.groups = []
        self.last_category: Category | None = None
        self.last_category_pos: int = -1
        self.add_group()

        controls = Frame(self.master)
        category_info = Frame(self.master)
        category_info.pack(fill="x")
        self.outer.pack(fill="both", expand=True)
        controls.pack(fill="x")

        self.category_name_var = StringVar(category_info, value="")
        Label(category_info, text="Category Name:").grid(row=0, column=0, sticky=W, padx=4)
        Entry(category_info, textvariable=self.category_name_var).grid(row=0, column=1)

        Button(controls, text="Add Group", command=self.add_group).pack(side="left")
        self.create_btn = Button(controls, text="Create Category", command=self.create_category)
        self.create_btn.pack(side="left", padx=3)
        self.query_btn = Button(controls, text="Query Category", command=self.load_category)
        self.query_btn.pack(side="left", padx=3)
        self.update_btn = Button(controls, text="Update Category", command=self.edit_category, state="disabled")
        self.update_btn.pack(side="left", padx=3)
        self.delete_btn = Button(controls, text="Delete Category", command=self.delete_category, state="disabled")
        self.delete_btn.pack(side="left", padx=3)
        self.free_btn = Button(controls, text="Free Category", command=self.free_category, state="disabled")
        self.free_btn.pack(side="left", padx=3)

    def destroy(self):
        self.outer.unbind_scroll()
        self.base.show()
        self.master.destroy()

    def refresh_layout(self):
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

    def delete_all(self):
        for g in self.groups:
            self.delete_group(g)

    def add_group(self, col: CategoryColumn = None):
        frame = Frame(self.scroll_frame, bd=2, relief="groove", padx=5, pady=5)

        name_entry_var = StringVar(frame, value="" if col is None else col.display_name)
        name_entry = Entry(frame, textvariable=name_entry_var)
        type_var = StringVar(frame, value=self.options[0] if col is None else col.col_type)
        type_entry = OptionMenu(frame, type_var,*self.options)
        primary_key_var = BooleanVar(frame, value=False if col is None else col.is_primary_key)
        primary_key = Checkbutton(frame, variable=primary_key_var)
        text_area_var = BooleanVar(frame, value=False if col is None else col.text_area)
        text_area = Checkbutton(frame, variable=text_area_var)
        incrementer_var = BooleanVar(frame, value=False if col is None else col.incrementer)
        incrementer = Checkbutton(frame, variable=incrementer_var)
        decimal_digits = NumberEntry(frame, value=0 if col is None else col.decimal_digits)


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
            "name": name_entry_var,
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
        category.display_name = self.category_name_var.get().strip()
        category.db_name = db_name(category.display_name)
        temp_col_names = []
        for i, g in enumerate(self.groups):
            name: str = g["name"].get().strip()
            if name.count("->") > 0:
                show_error(f"String '->' not allowed in column name '{name}' (reserved for renaming)")
                return
            col_type, col_prim, text_area, decimal_digits, incrementer = self._extract_group_values(g)
            if col_prim:
                has_primary_key = True
            db_col_name = db_name(name)
            if db_col_name in temp_col_names:
                show_error(f"Field '{name}' would create a duplicate column name in database")
                return
            temp_col_names.append(db_col_name)
            category.columns.append(CategoryColumn(db_col_name, name, col_prim, col_type, decimal_digits,
                                                   incrementer, text_area))
        if not has_primary_key:
            show_error("Category can't be added\nMissing at least one primary key")
            return
        if len(temp_col_names) < 1:
            show_error("Can't create Category without columns")
            return
        if not category.add_category():
            return
        self.base.add_category(category)
        show_info("Category successfully added ")

    def load_category(self):
        self.last_category = None
        for pos, cat in enumerate(self.base.categories):
            if cat.display_name == self.category_name_var.get().strip():
                self.last_category = cat
                self.last_category_pos = pos
                break
        if self.last_category is None:
            show_error("No category with given name")
            return
        self.delete_all()
        for col in self.last_category.columns:
            self.add_group(col)
        self.set_to_modify_state()

    def edit_category(self):
        has_primary_key = False
        category = Category()
        category.display_name = self.category_name_var.get().strip()
        category.db_name = db_name(category.display_name)
        temp_col_names = []
        for i, g in enumerate(self.groups):
            name: str = g["name"].get().strip()
            if name.count("->") > 1:
                show_error(f"Can't rename column '{name}' multiple times.")
                return
            col_type, col_prim, text_area, decimal_digits, incrementer = self._extract_group_values(g)
            if col_prim:
                has_primary_key = True
            local_db_name = db_name(name.split("->")[-1].strip())
            if local_db_name in temp_col_names:
                show_error(f"Field '{name}' would create a duplicate column name in database")
                return
            temp_col_names.append(local_db_name)
            category.columns.append(CategoryColumn(local_db_name, name, col_prim, col_type, decimal_digits,
                                                   incrementer, text_area))
        if not has_primary_key:
            show_error("Category can't be added\nMissing at least one primary key")
            return
        if not self.last_category.update_category(category):
            return
        self.base.categories[self.last_category_pos] = self.last_category
        show_info("Category successfully updated")

    def delete_category(self):
        if not self.last_category.delete():
            return
        self.base.delete_group(self.last_category_pos)
        self.last_category = None
        self.create_and_query()
        show_info("Category successfully deleted")

    def free_category(self):
        self.last_category = None
        self.create_and_query()

    @staticmethod
    def _extract_group_values(g: dict) -> tuple[str, bool, bool, int, bool]:
        col_type: str = g["type"].get()
        col_prim: bool = g["is_key"].get()
        text_area: bool = g["text_area"].get() and col_type == TableTypes.STRING
        try:
            decimal_digits = int(g["decimal_digits"].get()) if col_type == TableTypes.INTEGER else 0
        except ValueError:
            decimal_digits = 0
        incrementer = col_type == TableTypes.INTEGER and decimal_digits == 0 and g["incrementer"].get()
        return col_type, col_prim, text_area, decimal_digits, incrementer

    def create_and_query(self):
        self.create_btn.configure(state="active")
        self.query_btn.configure(state="active")
        self.update_btn.configure(state="disabled")
        self.delete_btn.configure(state="disabled")
        self.free_btn.configure(state="disabled")

    def set_to_modify_state(self):
        self.create_btn.configure(state="disabled")
        self.query_btn.configure(state="disabled")
        self.update_btn.configure(state="active")
        self.delete_btn.configure(state="active")
        self.free_btn.configure(state="active")


class EntryManipulator:
    def __init__(self, base: BaseInterface, category: Category):
        self.base = base
        self.category = category
        base.hide()

        self.master = Toplevel()
        self.master.protocol("WM_DELETE_WINDOW", self.destroy)
        self.master.title("Create Entry into " + category.display_name)
        self.master.minsize(550, 400)
        self.master.focus_force()

        self.dropdown_text = StringVar(self.master, "Add Entry")
        self.keep_entries = BooleanVar(self.master, False)
        self.dropdown_text.trace_add("write", self.on_update)
        self.mode_dict = {
            "Add Entry": lambda : self.set_button_for_add_entry(),
            "Edit Entry": lambda: self.set_button_for_modify_entry(),
            "Delete Entry": lambda: self.set_button_for_delete_entry()
        }
        Label(self.master, text="Edit Mode:").grid(row=0, column=0, sticky=W, padx=4)
        dropdown = OptionMenu(self.master, self.dropdown_text,
                              *list(self.mode_dict.keys()))
        dropdown.grid(column=1, row=0, pady=5, sticky=W)
        Label(self.master, text="Keep Values:").grid(row=1, column=0, sticky=W, padx=4)
        Checkbutton(self.master, text="Keep Field values after adding/editing/freeing", variable=self.keep_entries) \
            .grid(row=1, column=1, sticky=W)

        self.var_elements = []
        self.elements = []
        self.default_values = []
        self.queried_entry = []

        pos = 2
        for col in category.columns:
            if col.col_type == TableTypes.STRING and col.text_area:
                entry = TKScrollText(self.master, wrap=WORD, height=3, width=30)
                self.default_values.append("")
                self.var_elements.append(ScrollText(entry))
            elif col.col_type == TableTypes.STRING:
                var = StringVar(self.master)
                self.default_values.append("")
                self.var_elements.append(var)
                entry = Entry(self.master, textvariable=var, width=30)
            elif col.col_type == TableTypes.BOOLEAN:
                var = BooleanVar(self.master)
                self.default_values.append(False)
                self.var_elements.append(var)
                entry = Checkbutton(self.master, variable=var)
            elif col.col_type == TableTypes.INTEGER:
                entry = NumberEntry(self.master, col.decimal_digits,None, width=15)
                self.default_values.append(None)
                self.var_elements.append(entry)
            else:
                continue
            Label(self.master, text=col.display_name+":").grid(row=pos, column=0, sticky=W, padx=4, pady=3)
            entry.grid(row=pos, column=1, sticky=W)
            self.elements.append(entry)
            pos += 1

        frame = Frame(self.master)
        frame.grid(row=pos, column=0, columnspan=2, pady=5)
        self.btn_one = Button(frame)
        self.btn_one.grid(row=pos, column=0)
        self.btn_two = Button(frame)
        self.btn_two.grid(row=pos, column=1, padx=5, sticky=W)
        self.btn_three = Button(frame)
        self.btn_three.grid(row=pos, column=2, padx=5, sticky=W)
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
        values = [e if not isinstance(e, str) else e.strip() for e in values]
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
        values = [e if not isinstance(e, str) else e.strip() for e in values]
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
        self.reset(True)
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
        self.base.show()
        self.master.destroy()

    def add_to_db(self):
        values = [e.get() for e in self.var_elements]
        values = [e if not isinstance(e, str) else e.strip() for e in values]
        if not self.category.add_entry(values):
            return
        if self.keep_entries.get():
            show_info("Entry successfully added")
        self.reset()

    def reset(self, always_delete: bool = False):
        if self.keep_entries.get() and not always_delete:
            return
        for i, element in enumerate(self.var_elements):
            element.set(self.default_values[i])


class TableView:
    def __init__(self, category: Category):
        self.category = category
        self.master = Toplevel()
        self.row_amount = 10
        self.current_page = 1
        self.master.title("View " + category.display_name)
        self.master.minsize(550, 400)
        self.master.focus_force()
        self.query_result: list[list[Any]] = []
        self.pages: list[list[list[Any]]] = []

        self.queries = [Entry(self.master) for _ in range(len(self.category.col_display_names))]
        [item.grid(row=2, column=i, pady=(5, 10)) for i, item in enumerate(self.queries)]
        self.table = SimpleTable(self.master, [self.category.col_display_names],
                            category.incrementer_list, self.update_label)
        self.table.grid(row=3, column=0, columnspan=len(self.category.col_display_names))
        btn_frame = Frame(self.master)
        self.btn_next = Button(btn_frame, text="→", command=self.next_page)
        self.btn_next.grid(row=0, column=2, pady=(5,0))
        self.btn_previous = Button(btn_frame, text="←", command=self.previous_page)
        self.btn_previous.grid(row=0, column=0, pady=(5,0))
        self.page_label = Label(btn_frame, text="1/?")
        self.page_label.grid(row=0, column=1, pady=(5,0), padx=3)
        btn_frame.grid(row=0,column=0, padx=(20,0), sticky=W)
        self.btn_search = Button(self.master, text="Search", command=self.search_with_info)
        self.btn_search.grid(row=1, column=0, pady=(5,0), padx=(20,0), sticky=W)

        self.query_first_page()
        self.master.update_idletasks()

    def next_page(self):
        if self.current_page >= len(self.pages):
            return
        self.pages[self.current_page-1] = self.query_result.copy()
        self.current_page += 1
        self.query_result = self.pages[self.current_page-1].copy()
        self.update_rows()

        if self.current_page > 1:
            self.btn_previous.config(state="active")
        if self.current_page >= len(self.pages):
            self.btn_next.config(state="disabled")
        self.page_label.configure(text=f"{self.current_page}/{len(self.pages)}")

    def previous_page(self):
        if self.current_page < 2:
            return
        self.pages[self.current_page-1] = self.query_result.copy()
        self.current_page -= 1
        self.query_result = self.pages[self.current_page-1].copy()
        self.update_rows()

        if self.current_page < 2:
            self.btn_previous.config(state="disabled")
        if self.current_page < len(self.pages):
            self.btn_next.config(state="active")
        self.page_label.configure(text=f"{self.current_page}/{len(self.pages)}")

    def update_rows(self):
        self.table.resize_table(1+len(self.query_result))
        for row, row_data in enumerate(self.category.transform_query_into_string(self.query_result)):
            for col, data in enumerate(row_data):
                self.table.set(row + 1, col, data)

    def search_with_info(self):
        self.query_first_page()

    def query_first_page(self):
        self.pages = []
        self.current_page = 1
        self.btn_previous.config(state="disabled")
        self.query_result, next_page = self.category.query_first_page("Could not load first page for category",
                                                                      self.row_amount, self.get_queries_text())
        self.btn_next.config(state="active" if next_page else "disabled")
        if self.query_result is None:
            self.master.destroy()
        self.update_rows()
        self.pages.append(self.query_result)
        other_pages = self.category.query_all_other_pages("Could not load rest of pages for category",
                                                          self.row_amount, self.get_queries_text())
        if self.query_result is None:
            self.master.destroy()
        for entry in other_pages:
            self.pages.append(entry)
        self.page_label.configure(text=f"{self.current_page}/{len(self.pages)}")

    def get_queries_text(self) -> list[str]:
        return [e.get() for e in self.queries]

    def update_label(self, row: int, col_pos: int, increment: bool,
                     widget_to_update: SelectableLabel) -> None:
        column = self.query_result[row]
        col_copy = self.query_result[row].copy()
        if col_copy[col_pos] is None:
            col_copy[col_pos] = 1 if increment else 0
        else:
            col_copy[col_pos] += 1 if increment else (-1 if col_copy[col_pos] > 0 else 0)
        if not self.category.do_partial_update(column, col_copy[col_pos], col_pos):
            return
        widget_to_update.config(text=str(col_copy[col_pos]))
        self.query_result[row] = col_copy


class DataImporter:
    def __init__(self, base: BaseInterface):
        self.base = base
        base.hide()

        self.__LETTERBOXD: Final[str] = "Letterboxd"
        self.__ANILIST: Final[str] = "MyAnimeList (Anime)"
        self.options = [self.__LETTERBOXD, self.__ANILIST]

        self.master = Toplevel()
        self.master.protocol("WM_DELETE_WINDOW", self.destroy)
        self.master.title("Import Data")
        self.master.minsize(550, 400)
        self.master.focus_force()

        self.option_string = StringVar(self.master, self.options[0])
        self.table_name = StringVar(self.master)
        self.file_path = StringVar(self.master)
        self.selectable_option = OptionMenu(self.master, self.option_string, *self.options)

        Label(self.master, text="Table Name:").grid(column=0, row=0, padx=5, sticky=W)
        Label(self.master, text="File Path:").grid(column=0, row=1, padx=5, sticky=W)
        Label(self.master, text="Format:").grid(column=0, row=2, padx=5, sticky=W)

        Entry(self.master, textvariable=self.table_name, width=25).grid(column=1, row=0, pady=5)
        Entry(self.master, textvariable=self.file_path, width=25).grid(column=1, row=1, pady=5)
        self.selectable_option.grid(column=1, row=2, pady=5, sticky=W)

        Button(self.master, text="Import Data", command=self.execute).grid(column=0, row=3, pady=5, sticky=W)
        Button(self.master, text="Choose a file", command=self.set_path).grid(column=2, row=1, padx=5)

    def destroy(self):
        self.base.show()
        self.master.destroy()

    def set_path(self):
        self.file_path.set(askopenfilename())

    def execute(self):
        success = False
        match self.option_string.get():
            case self.__LETTERBOXD:
                temp = LetterBoxd(self.base, self.table_name.get().strip(), self.file_path.get())
                success = temp.success
            case self.__ANILIST:
                temp = MyAnimeList(self.base, self.table_name.get().strip(), self.file_path.get())
                success = temp.success

        if success:
            self.table_name.set("")
            self.file_path.set("")
            show_info("Import successful")


class BaseInterface:
    def __init__(self):
        self.master = Tk()
        self.master.title("Main Menu")
        self.master.minsize(550, 400)

        default_font = tk_font.nametofont("TkDefaultFont")
        default_font.configure(size=14)
        self.master.option_add("*Font", default_font)

        self.groups = []
        self.outer = ScrollableFrame(self.master)
        self.scroll_frame = self.outer.inner
        self.categories = Category.load_categories_from_db()
        self.interface()
        for i in range(len(self.categories)):
            self.add_group(i)
        self.master.mainloop()

    def get_category(self, disp_name: str) -> Category | None:
        for cat in self.categories:
            if cat.display_name == disp_name:
                return cat
        return None

    def hide(self):
        self.outer.unbind_scroll()
        self.master.withdraw()

    def show(self):
        self.master.deiconify()
        self.outer.bind_scroll()

    def add_category(self, c: Category):
        self.categories.append(c)
        self.add_group(-1)

    def delete_category(self, pos: int):
        self.categories.pop(pos)
        self.delete_group(pos)

    def add_group(self, pos: int):
        cat = self.categories[pos]
        frame = LabelFrame(self.scroll_frame, text=cat.display_name, relief="groove", pady=5, padx=5)

        Button(frame, text='Edit Category', command=partial(self.open_edit, cat)).grid(row=0, column=0)
        Button(frame, text='Open View', command=partial(self.open_view, cat)).grid(row=0, column=1, padx=5)
        Button(frame, text='Create HTML', command=partial(self.create_html, cat)).grid(row=0, column=2)

        self.groups.append(frame)
        self.refresh_layout()

    def delete_group(self, group_pos: int):
        self.groups[group_pos].pack_forget()
        self.groups[group_pos].destroy()
        self.groups.pop(group_pos)
        self.refresh_layout()

    def refresh_layout(self):
        for g in self.groups:
            g.pack_forget()

        for g in self.groups:
            g.pack(fill="x", pady=5, padx=5)

    def interface(self):
        category_info = Frame(self.master)
        category_info.pack(fill="x")

        Button(category_info, text='Category Editor', command=lambda: CategoryEditor(self)) \
            .grid(row=0, column=0, padx=(5,0), pady=5)
        Button(category_info, text='Create HTML for all', command=lambda: HTMLGenerator(self)) \
            .grid(row=0, column=1, padx=5)
        Button(category_info, text='Import Data', command=lambda: DataImporter(self)) \
            .grid(row=0, column=2)
        Button(category_info, text='Save to DB', command=lambda: update_disk_db(False)) \
            .grid(row=0, column=3 ,padx=5)
        self.outer.pack(fill="both", expand=True)

    @staticmethod
    def open_view(cat: Category) -> None:
        TableView(cat)

    def open_edit(self, cat: Category) -> None:
        EntryManipulator(self, cat)

    def create_html(self, cat: Category) -> None:
        HTMLGenerator(self, cat)


# ------------------------------------
# HTML Class Interactions and Variables
# ------------------------------------


CSS = '''
.data-table {
  margin: 0 auto;
  max-width: max(1000px, 75vw);
  border-collapse: collapse;
  table-layout: auto;
}

.data-table th,
.data-table td {
  border: 1px solid #000;
  padding: 8px 12px;
  vertical-align: top;

  /* allow wrapping instead of expanding */
  white-space: normal;
  overflow-wrap: anywhere;
  word-break: break-word;
}

/* max cell width = 2 * average column width */
.data-table td,
.data-table th {
  min-width: calc(50cqw / var(--cols));
  max-width: calc(200cqw / var(--cols));
}

/* header */
.data-table thead th {
  background: #dbeeff; /* light blue */
  font-weight: bold;
}

/* alternating row colors */
.data-table tbody tr:nth-child(odd) {
  background: #ffffff;
}

.data-table tbody tr:nth-child(even) {
  background: #f2f2f2; /* light gray */
}

h1 {
  text-align: center;
}

.links {
  display: flex;
  justify-content: center;
  align-items: center;
  flex-direction: column;
}

a {
  margin: 0.3em;
}
'''


class HTMLGenerator:
    def __init__(self, base: BaseInterface, category: Category = None):
        self.base = base
        self.file = open("./data.html", "w", encoding="UTF-8")
        self.title = "Complete Overview"
        if category is None:
            self.generate_full_html()
            self.file.close()
            return
        self.generate_partial_html(category)
        self.file.close()

    def generate_full_html(self):
        self.generate_header()
        self.generate_category_overview()
        for c in self.base.categories:
            self.generate_category(c)
        self.generate_footer()

    def generate_partial_html(self, cat: Category):
        self.title = f"Overview for {cat.display_name}"
        self.generate_header()
        self.generate_category(cat)
        self.generate_footer()

    def generate_header(self):
        self.file.write('<!DOCTYPE html><html lang="en"><head>'
                        '<meta charset="UTF-8">'
                        f'<title>{html.escape(self.title)}</title>'
                        f'<style>{CSS}</style>'
                        '</head><body>')
        pass

    def generate_category_overview(self):
        self.file.write('<div id="head_" class="links">')
        for c in self.base.categories:
            self.file.write(f'<a href="#{c.db_name}">Jump to {html.escape(c.display_name)}</a>')
        self.file.write('</div>')

    def generate_category(self, c: Category):
        data = c.query_full_table(f"Failed to retrieve data for category '{html.escape(c.display_name)}'")
        if data is None:
            return
        data = c.transform_query_into_string(data)
        self.file.write(f'<h1 id="{c.db_name}">{html.escape(c.display_name)} <a href="#head_">[Go back]</a></h1>'
                        f'<table class="data-table" style="--cols: {len(c.columns)}">'
                        '<thead><tr>')
        for col_name in c.col_display_names:
            self.file.write(f'<th>{html.escape(col_name)}</th>')
        self.file.write('</tr></thead>')

        regex = re.compile(r"https?://[^\s\"]+", flags=re.MULTILINE | re.UNICODE)

        def replace_link(m: re.Match[str]) -> str:
            return f'<a href="{html.unescape(m.group())}">{m.group()}</a>'

        for row in data:
            self.file.write('<tr>')
            for text in row:
                text = html.escape(text)
                self.file.write(f'<td>{regex.sub(replace_link, text)}</td>')
            self.file.write('</tr>')
        self.file.write('</tbody></table>')

    def generate_footer(self):
        self.file.write('</body></html>')


# ------------------------------------
# Importer
# ------------------------------------


class MyAnimeList:
    def __init__(self, base: BaseInterface, category_name: str, file_path: str):
        self.base = base
        self.success = False
        self.positions: dict[int, int] = {}
        self.category = self._get_category(category_name)
        if self.category is None:
            show_error("Could not create category or category with name isn't suitable for updates")
            return
        self.base_row: list[Any] = self.category.get_default_row()
        try:
            self.data = self._get_data(file_path)
        except Exception as err:
            show_error(f"Could not extract valid data from the given ZIP archive:\n{err}")
            return
        self.success = self._upsert_data()

    def _get_category(self, category_name: str) -> Category | None:
        category = self.base.get_category(category_name)
        if category is not None:
            if category.has_column_definition(["ANIMEDB ID", "ANIMEDB", "ANIMEDBID", "ANIMEID", "ANIME ID"],
                                              TableTypes.STRING) and \
                    category.has_column_definition(["SERIES TITLE", "SERIES", "TITLE"],
                                                   TableTypes.STRING) and \
                    category.has_column_definition("EPISODES", TableTypes.INTEGER, 0) and \
                    category.has_column_definition(["EPISODES WATCHED", "WATCHED EPISODES"], TableTypes.INTEGER, 0):
                self.positions = self._get_col_positions(category)
                return category
            return None
        category = Category()
        category.display_name = category_name
        category.db_name = db_name(category_name)
        category.columns = [CategoryColumn("AnimeDB ID", "AnimeDB_ID", True,
                                           TableTypes.STRING, 0, False, False), # 1
                            CategoryColumn("Series Title", "Series_Title", False,
                                           TableTypes.STRING, 0, False, True), # 2
                            CategoryColumn("Type", "Type", False,
                                           TableTypes.STRING, 0, False, False), # 3
                            CategoryColumn("Episodes", "Episodes", False,
                                           TableTypes.INTEGER, 0, False, False), # 4
                            CategoryColumn("My ID", "My_ID", False,
                                           TableTypes.STRING, 0, False, False), # 5
                            CategoryColumn("Episodes Watched", "Episodes_Watched", False,
                                           TableTypes.INTEGER, 0, True, False), # 6
                            CategoryColumn("Start Date", "start_date", False,
                                           TableTypes.STRING, 0, False, False), # 7
                            CategoryColumn("Finish Date", "finish_date", False,
                                           TableTypes.STRING, 0, False, False), # 8
                            CategoryColumn("My Rating", "my_rating", False,
                                           TableTypes.STRING, 0, False, False), # 9
                            CategoryColumn("My Score", "my_score", False,
                                           TableTypes.STRING, 0, False, False), # 10
                            CategoryColumn("DVD", "dvd", False,
                                           TableTypes.STRING, 0, False, False), # 11
                            CategoryColumn("My Storage", "my_storage", False,
                                           TableTypes.STRING, 0, False, False), # 12
                            CategoryColumn("Status", "status", False,
                                           TableTypes.STRING, 0, False, False), # 13
                            CategoryColumn("Comment", "comment", False,
                                           TableTypes.STRING, 0, False, True), # 14
                            CategoryColumn("Times Watched", "times_watched", False,
                                           TableTypes.INTEGER, 0, True, False), # 15
                            CategoryColumn("Rewatch Value", "rewatch_value", False,
                                           TableTypes.STRING, 0, False, False), # 16
                            CategoryColumn("Tags", "tags", False,
                                           TableTypes.STRING, 0, False, True), # 17
                            CategoryColumn("Rewatching", "rewatching", False,
                                           TableTypes.BOOLEAN, 0, True, False), # 18
                            CategoryColumn("Episodes Rewatching", "episodes_rewatching", False,
                                           TableTypes.INTEGER, 0, True, False), # 19
                            ]
        self.positions = {i: i for i in range(len(category.columns))}
        if not category.add_category():
            return None
        self.base.add_category(category)
        return category

    @staticmethod
    def _get_col_positions(category: Category) -> dict[int, int]:
        result = {
            0: category.column_position(["ANIMEDB ID", "ANIMEDB", "ANIMEDBID", "ANIMEID", "ANIME ID"],
                                           TableTypes.STRING),
            1: category.column_position(["SERIES TITLE", "SERIES", "TITLE"], TableTypes.STRING),
            2: category.column_position("Type", TableTypes.STRING),
            3: category.column_position("EPISODES", TableTypes.INTEGER, 0),
            4: category.column_position(["My ID", "ID"], TableTypes.STRING),
            5: category.column_position(["EPISODES WATCHED", "WATCHED EPISODES"], TableTypes.INTEGER, 0),
            6: category.column_position("Start Date", TableTypes.STRING),
            7: category.column_position("Finish Date", TableTypes.STRING),
            8: category.column_position(["My Rating", "Rating"], TableTypes.STRING),
            9: category.column_position(["My Score", "Score"], TableTypes.STRING),
            10: category.column_position("DVD", TableTypes.STRING),
            11: category.column_position(["My Storage", "Storage"], TableTypes.STRING),
            12: category.column_position("Status", TableTypes.STRING),
            13: category.column_position("Comment", TableTypes.STRING),
            14: category.column_position("Times Watched", TableTypes.INTEGER, 0),
            15: category.column_position("Rewatch Value", TableTypes.STRING),
            16: category.column_position("Tags", TableTypes.STRING),
            17: category.column_position("Rewatching", TableTypes.BOOLEAN),
            18: category.column_position("Episodes Rewatching", TableTypes.INTEGER, 0),
        }
        result = {k: v for k, v in result.items() if v != -1}
        return result

    @staticmethod
    def _get_data(file_path: str) -> list[list[Any]]:
        def to_int(x: str) -> int | None:
            try:
                return int(x)
            except ValueError:
                return None

        def to_bool(x: str) -> bool:
            return x.upper().strip() in ["1", "TRUE"]

        row_position: dict[str, tuple[int, Callable[[str],Any]]] = {
            "series_animedb_id": (0, lambda x: x),
            "series_title": (1, lambda x: x),
            "series_type": (2, lambda x: x),
            "series_episodes": (3, to_int),
            "my_id": (4, lambda x: x),
            "my_watched_episodes": (5, to_int),
            "my_start_date": (6, lambda x: x),
            "my_finish_date": (7, lambda x: x),
            "my_rated": (8, lambda x: x),
            "my_score": (9, lambda x: x),
            "my_dvd": (10, lambda x: x),
            "my_storage": (11, lambda x: x),
            "my_status": (12, lambda x: x),
            "my_comments": (13, lambda x: x),
            "my_times_watched": (14, to_int),
            "my_rewatch_value": (15, lambda x: x),
            "my_tags": (16, lambda x: x),
            "my_rewatching": (17, to_bool),
            "my_rewatching_ep": (18, to_int),
        }
        anime_list: list[list[str]] = []
        base_empty = [func("") for _, func in row_position.values()]

        root = ElTree.parse(file_path)
        for anime in root.findall("anime"):
            entry = base_empty.copy()
            for child in anime:
                if child.tag in row_position:
                    pos, func = row_position[child.tag]
                    entry[pos] = func(child.text if child.text is not None else "")
            anime_list.append(entry)
        return anime_list

    def _upsert_data(self) -> bool:
        for row in self.data:
            for key, value in self.positions.items():
                self.base_row[value] = row[key]
            if not self.category.upsert_entry(self.base_row):
                return False
        return True


csv.register_dialect("letterboxd", delimiter=",", lineterminator="\n", quoting=csv.QUOTE_MINIMAL, strict=True)


class LetterBoxd:
    def __init__(self, base: BaseInterface, category_name: str, zip_archive_path: str):
        self.base = base
        self.success = False
        self.positions: dict[int, int] = {}
        self.category = self._get_category(category_name)
        if self.category is None:
            show_error("Could not create category or category with name isn't suitable for updates")
            return
        self.base_row: list[Any] = self.category.get_default_row()
        try:
            self.data = self._get_data(zip_archive_path)
        except Exception as err:
            show_error(f"Could not extract valid data from the given ZIP archive:\n{err}")
            return
        self.success = self._upsert_data()

    def _get_category(self, category_name: str) -> Category | None:
        category = self.base.get_category(category_name)
        if category is not None:
            if category.has_column_definition(["NAME", "TITLE"], TableTypes.STRING) and \
                category.has_column_definition(["YEAR", "RELEASE YEAR"], TableTypes.INTEGER, 0) and \
                category.has_column_definition("WATCHED", TableTypes.BOOLEAN):
                self.positions = self._get_col_positions(category)
                return category
            return None
        category = Category()
        category.display_name = category_name
        category.db_name = db_name(category_name)
        category.columns = [CategoryColumn("Date", "Date", False,
                                      TableTypes.STRING, 0, False, False), # 1
                       CategoryColumn("Name", "Name", True,
                                      TableTypes.STRING, 0, False, False), # 2
                       CategoryColumn("Year", "Year", True,
                                      TableTypes.INTEGER, 0, False, False), # 3
                       CategoryColumn("Letterboxd_URI", "Letterboxd URI", False,
                                      TableTypes.STRING, 0, False, True), # 4
                       CategoryColumn("Watched", "Watched", False,
                                      TableTypes.BOOLEAN, 0, False, True), # 5
                       ]
        self.positions = {i: i for i in range(len(category.columns))}
        if not category.add_category():
            return None
        self.base.add_category(category)
        return category

    @staticmethod
    def _get_col_positions(category: Category) -> dict[int, int]:
        result = {
            0: category.column_position("DATE", TableTypes.STRING),
            1: category.column_position(["NAME", "TITLE"], TableTypes.STRING),
            2: category.column_position(["YEAR", "RELEASE YEAR"], TableTypes.INTEGER, 0),
            3: category.column_position(["LETTERBOXD URI", "LETTERBOXD URL", "LETTERBOXD"],
                                             TableTypes.STRING),
            4: category.column_position("WATCHED", TableTypes.BOOLEAN),
        }
        result = {k: v for k, v in result.items() if v != -1}
        return result

    @staticmethod
    def _get_data(zip_archive_path: str) -> list[list[Any]]:
        archive = zipfile.ZipFile(zip_archive_path, "r")
        result: list[list] = [["Date"], ["Name"], ["Year"], ["Letterboxd URI"]]
        watched = [list(e) for e in
                   zip(*[data for data in csv.reader(io.TextIOWrapper(archive.open("watched.csv", "r"),
                                                     encoding="UTF-8"), "letterboxd")], strict=True)]
        planned = [list(e) for e in
                   zip(*[data for data in csv.reader(io.TextIOWrapper(archive.open("watchlist.csv", "r"),
                                                     encoding="UTF-8"), "letterboxd")], strict=True)]
        for pos, (col, *_) in enumerate(result.copy()):
            result[pos] = []
            for item_list in watched:
                if len(item_list) > 0 and item_list[0] == col:
                    result[pos] = item_list[1:]

        result.append([True] * len(result[0]))

        amt = 0
        for pos, (col, *_) in enumerate(result.copy()):
            for item_list in planned:
                if len(item_list) > 0 and item_list[0] == col:
                    result[pos].extend(item_list[1:])
                    amt = len(item_list) - 1

        result[-1].extend([False] * amt)

        return [[date, name, int(year), uri, watch] for (date, name, year, uri, watch) in zip(*result, strict=True)]

    def _upsert_data(self) -> bool:
        for row in self.data:
            for key, value in self.positions.items():
                self.base_row[value] = row[key]
            if not self.category.upsert_entry(self.base_row):
                return False
        return True


# ------------------------------------
# MAIN
# ------------------------------------


def main():
    BaseInterface()


if __name__ == '__main__':
    main()
