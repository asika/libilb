import ConfigParser

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, DateTime, Text, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func, desc

from models.book_scrape import bookScrape
from book.v1 import extract, es

def main(**kwargs):
    debug = kwargs.get("debug", False)

    def _override_cfg(cfg, *args, **kwargs):
        section, kw, default = args
        cfg.set(section, kw, kwargs.get(kw, default))

    config = ConfigParser.RawConfigParser(allow_no_value=True)
    # init defaults
    config.add_section("mysql")
    _override_cfg(config, "mysql", "protocol", "mysql+mysqldb")
    _override_cfg(config, "mysql", "host", "127.0.0.1")
    _override_cfg(config, "mysql", "port", 3306)
    _override_cfg(config, "mysql", "db", "tpml")
    _override_cfg(config, "mysql", "user", "root")
    _override_cfg(config, "mysql", "password", "defaultpassword")

    config.add_section("general")
    _override_cfg(config, "general", "fetch_errors", False)

    try:
        with open(kwargs.get("cfg", "default.cfg")) as fp:
            config.readfp(fp)
    except IOError, e:
        print "IOError"

    protocol    = config.get("mysql", "protocol")
    host        = config.get("mysql", "host")
    port        = config.get("mysql", "port")
    db          = config.get("mysql", "db")
    user        = config.get("mysql", "user")
    password    = config.get("mysql", "password")

    engine = create_engine(
        "{protocol}://{user}:{password}@{host}:{port}/{db}?charset=utf8".format(
            protocol=protocol, host=host, port=port, db=db, user=user, password=password
        ),
        encoding="utf-8",
        echo=True
    )
    Session = sessionmaker(bind=engine)
    Session.configure(bind=engine)

    session = Session()

    q = session.query(bookScrape).filter(bookScrape.httpcode==200)

    if debug:
        book_id = kwargs.get("id", 1)

        q = q.filter(bookScrape.id==book_id)
        return q.one()

    # http://stackoverflow.com/questions/1145905/sqlalchemy-scan-huge-tables-using-orm
    for item in q.yield_per(10):
        es.main(
            item,
            record=extract.main(item)
        )

