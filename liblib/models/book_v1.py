import datetime

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, DateTime, Text, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func, desc

from .book_scrape import bookScrape

Base = declarative_base()

class Book(Base):
    __tablename__ = "book"
    __table_args__ = {'extend_existing': True}

    id = Column(Integer, primary_key=True)
    httpcode = Column(Integer)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
    content = Column(Text)

    id = Column(Integer, primary_key=True, autoincrement=True)
    tpml_id = Column(Integer)
    # https://zh.wikipedia.org/wiki/%E5%9B%BD%E9%99%85%E6%A0%87%E5%87%86%E4%B9%A6%E5%8F%B7
    isbn = Column(String(20))
    title = Column(String(200))
    author = Column(String(30))
    publisher = Column(String(30))
    year = Column(Integer)
    desc = Column(Text)


