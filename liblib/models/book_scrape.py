import datetime

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, DateTime, Text, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func, desc

Base = declarative_base()

class bookScrape(Base):
    __tablename__ = "book_scrape"
    __table_args__ = {'extend_existing': True}

    id = Column(Integer, primary_key=True)
    httpcode = Column(Integer)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
    content = Column(Text)

class scrapeSettings(Base):
    __tablename__ = "scrape_settings"

    key = Column(String(100), primary_key=True)
    value = Column(String(100))

