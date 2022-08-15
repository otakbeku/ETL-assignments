import sqlite3
from typing import Optional
from sqlalchemy.engine import create

from sqlmodel import Field, SQLModel, create_engine, Session, select
import os

import secrets

default_sqlite_file_name = "dataset.db"

# sqlite_url = f"sqlite://{default_sqlite_file_name}"

class Flow():
    def __init__(self, sqlite_file_name:str=None, echo:bool=True):
        self.sqlite_file_name = default_sqlite_file_name
        if sqlite_file_name:
            self.sqlite_file_name = sqlite_file_name    
        self.sqlite_url = f"sqlite:///{self.sqlite_file_name}"
        self.engine = create_engine(self.sqlite_url, echo=echo)
        if not (os.path.exists(self.sqlite_file_name)):
            print("Database is not found. Creating a new one")
            SQLModel.metadata.create_all(self.engine)

    def insert(self, data:list=None):
        if data:
            with Session(self.engine) as sess:
                sess.add_all(data)
                sess.commit()
                sess.close()
    
    def bulk_insert(self, data:list=None):
        if data:
            with Session(self.engine) as sess:
                sess.add_all(data)
                sess.commit()
                sess.close()
    
    def single_insert(self, data):
        if data:
            with Session(self.engine) as sess:
                sess.add(data)
                sess.commit()
                sess.close()
    
    # TODO: multiple 'where' clauses
    def select_where(self, table:str, query:dict):
        if query and table:
            attribute, value = list(query.items())[0]
            with Session(self.engine) as sess:
                target_table = globals()[table]
                statement = select(target_table).where(getattr(target_table,attribute)==value)
                temp = sess.exec(statement)
                results = temp.all()
                sess.close()
            return results
    def select(self, table:str):
        if table:
            with Session(self.engine) as sess:
                target_table = globals()[table]
                statement = select(target_table)
                temp = sess.exec(statement)
                results = temp.all()
                sess.close()
            return results