from __future__ import annotations

import re
import sqlite3
from mimetypes import read_mime_types
from tkinter import *

class NumberEntry(Entry):
    def __init__(self, master=None, decimal_digits: int = 0, value: int = 0, **kwargs):
        self.var = StringVar()
        self.decimal_digits: int = decimal_digits if decimal_digits > 0 else 0
        self.format: str = r"^[0-9]*(\.[0-9]{0,"+str(decimal_digits)+r"})?$" if decimal_digits > 0 else r"^[0-9]*$"
        self.var.set(format_number(value, decimal_digits))

        Entry.__init__(self, master, textvariable=self.var, **kwargs)
        self.old_value: str = self.var.get()
        self.var.trace('w', self.check)
        self.get, self.set = self.var.get, self.var.set

    def check(self, *_):
        if re.search(self.format, self.get(), flags=re.MULTILINE | re.UNICODE):
            self.old_value = self.get()
        else:
            # there's non-digit characters in the input; reject this
            self.set(self.old_value)

    def get_num_value(self) -> int:
        int_string = self.get()
        if int_string == "" or int_string == '.':
            return 0
        if self.decimal_digits == 0:
            return int(self.get())
        int_string = "0" + int_string
        if int_string.count('.') == 0 or int_string.endswith("."):
            return int(int_string.removesuffix(".")) * (10 ** self.decimal_digits)
        front, back = int_string.split(".")
        back += "0" * (len(back) - self.decimal_digits)
        return int(front + back)

def format_number(value: int, decimal_digits: int) -> str:
    if decimal_digits < 1:
        return str(value)
    base_string = str(value)
    base_string = ((decimal_digits - len(base_string) + 1) * "0") + base_string
    return base_string[:-decimal_digits] + "." + base_string[-decimal_digits:]

def trim_after_format(value: int, decimal_digits: int) -> str:
    base_string = format_number(value, decimal_digits)
    base_string = base_string.rstrip("0")
    return base_string.rstrip(".")


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
        except sqlite3.IntegrityError:
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


class ISBNBookInterface:
    def __init__(self):
        self.master = Tk()
        self.keep_title = BooleanVar()
        self.keep_subtitle = BooleanVar()
        self.keep_series = BooleanVar()
        self.keep_series_number = BooleanVar()
        self.keep_author = BooleanVar()
        self.keep_checked_read = BooleanVar()
        self.keep_comment = BooleanVar()
        self.str_isbn = StringVar()
        self.str_title = StringVar()
        self.str_subtitle = StringVar()
        self.str_series = StringVar()
        self.str_series_number = StringVar()
        self.str_author = StringVar()
        self.checked_read = BooleanVar()
        self.str_comment = StringVar()
        # Normally you scan books you have read.
        self.checked_read.set(True)

    def update_db(self):
        isbn = self.str_isbn.get().strip() or None
        book_name = self.str_title.get().strip() or None
        subtitle = self.str_subtitle.get().strip() or None
        series = self.str_series.get().strip() or None
        number = self.str_series_number.get().strip() or None
        author = self.str_author.get().strip() or None
        read = self.checked_read.get() == 1
        comment = self.str_comment.get().strip() or None
        if number is not None:
            regex = re.compile(r"^\d+\.\d$|^\d+$")
            if not regex.match(number):
                print("Invalid series number")
                return
            number_split = number.split(".")
            number = int(number_split[0]) * 10
            if len(number_split) == 2:
                number += int(number_split[1])

        cur = con.cursor()
        try:
            pass
        except sqlite3.IntegrityError:
            print("DB update failed")
            return
        con.commit()
        print("Book updated")

    def interface(self):
        isbn = Entry(self.master, textvariable=self.str_isbn, width=55)
        title = Entry(self.master, textvariable=self.str_title, width=55)
        subtitle = Entry(self.master, textvariable=self.str_subtitle, width=55)
        series = Entry(self.master, textvariable=self.str_series, width=55)
        series_number = Entry(self.master, textvariable=self.str_series_number, width=55)
        author = Entry(self.master, textvariable=self.str_author, width=55)
        comment = Entry(self.master, textvariable=self.str_comment, width=55)

        self.master.title('Enter Book')

        Label(self.master, text='ISBN').grid(row=0)
        isbn.grid(row=0, column=1)

        Label(self.master, text='Title').grid(row=1)
        title.grid(row=1, column=1)
        Checkbutton(self.master, text='keep', variable=self.keep_title).grid(row=2, column=1)

        Label(self.master, text='Subtitle').grid(row=3)
        subtitle.grid(row=3, column=1)
        Checkbutton(self.master, text='keep', variable=self.keep_subtitle).grid(row=4, column=1)

        Label(self.master, text='Series').grid(row=5)
        series.grid(row=5, column=1)
        Checkbutton(self.master, text='keep', variable=self.keep_series).grid(row=6, column=1)

        Label(self.master, text='Series Number').grid(row=7)
        series_number.grid(row=7, column=1)
        Checkbutton(self.master, text='keep', variable=self.keep_series_number).grid(row=8, column=1)

        Label(self.master, text='Author').grid(row=9)
        author.grid(row=9, column=1)
        Checkbutton(self.master, text='keep', variable=self.keep_author).grid(row=10, column=1)

        Checkbutton(self.master, text='Read the Book', variable=self.checked_read).grid(row=11, column=1)
        Checkbutton(self.master, text='keep', variable=self.keep_checked_read).grid(row=12, column=1)

        Label(self.master, text='Author').grid(row=13)
        comment.grid(row=13, column=1)
        Checkbutton(self.master, text='keep', variable=self.keep_comment).grid(row=14, column=1)

        button_pos = 15
        Button(self.master, text='Look up ISBN', width=25).grid(row=button_pos,
                                                                                           columnspan=2)
        Button(self.master, text='Open Goodreads', width=25).grid(row=button_pos + 1,
                                                                                               columnspan=2)
        Button(self.master, text='Save to DB', width=25).grid(row=button_pos + 2,
                                                                                           columnspan=2)
        Button(self.master, text='Get DB Entry from ISBN', width=25).grid(row=button_pos + 3,
                                                                                                     columnspan=2)
        Button(self.master, text='Update DB Entry', width=25, command=self.update_db).grid(row=button_pos + 4,
                                                                                           columnspan=2)
        Button(self.master, text='View Books', width=25).grid(row=button_pos + 5,
                                                                                          columnspan=2)
        Button(self.master, text='Stop', width=25, command=self.master.destroy).grid(row=button_pos + 6,
                                                                                     columnspan=2)
        NumberEntry(self.master, decimal_digits=2, width=55).grid(row=button_pos + 7, columnspan=2)
        NumberEntry(self.master, decimal_digits=0, value=12, width=55).grid(row=button_pos + 8, columnspan=2)

        self.master.mainloop()


if __name__ == '__main__':
    inter = ISBNBookInterface()
    inter.interface()
