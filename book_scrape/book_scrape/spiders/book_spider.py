import datetime
import ConfigParser

import scrapy
from scrapy.selector import Selector

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

class bookSpider(scrapy.Spider):
    name = "book"

    custom_settings = {
        "ROBOTSTXT_OBEY": False
    }

    def __init__(self, *args, **kwargs):
        super(bookSpider, self).__init__(*args, **kwargs)

        def _override_cfg(cfg, *args, **kwargs):
            section, kw, default = args
            cfg.set(section, kw, kwargs.get(kw, default))

        self.config = ConfigParser.RawConfigParser(allow_no_value=True)
        # init defaults
        self.config.add_section("mysql")
        _override_cfg(self.config, "mysql", "protocol", "mysql+mysqldb")
        _override_cfg(self.config, "mysql", "host", "127.0.0.1")
        _override_cfg(self.config, "mysql", "port", 3306)
        _override_cfg(self.config, "mysql", "db", "tpml")
        _override_cfg(self.config, "mysql", "user", "root")
        _override_cfg(self.config, "mysql", "password", "defaultpassword")

        self.config.add_section("general")
        _override_cfg(self.config, "general", "fetch_errors", False)

        with open(kwargs.get("cfg", "default.cfg")) as fp:
            self.config.readfp(fp)

        self.init_db()

        max_index = int(kwargs.get("max_index", self.get_maxindex()))
        self.update_index(end_index=max_index)

    def init_db(self):
        protocol    = self.config.get("mysql", "protocol")
        host        = self.config.get("mysql", "host")
        port        = self.config.get("mysql", "port")
        db          = self.config.get("mysql", "db")
        user        = self.config.get("mysql", "user")
        password    = self.config.get("mysql", "password")

        self.engine = create_engine(
            "{protocol}://{user}:{password}@{host}:{port}/{db}?charset=utf8".format(
                protocol=protocol, host=host, port=port, db=db, user=user, password=password
            ),
            encoding="utf-8",
            echo=True
        )
        Base.metadata.create_all(self.engine)

        Session = sessionmaker(bind=self.engine)
        Session.configure(bind=self.engine)

        self.session = Session()

    def get_maxindex(self, **kwargs):
        """
        Fetch latest index from TPML website.
        """
        if kwargs.get("max_index", None):
            return int(kwargs.get("max_index"))
        else:
            q = self.session.query(scrapeSettings).filter(scrapeSettings.key == "max_index")
            max_index = 1
            try:
                max_index = int(q.one().pop())
            except Exception, e:
                self.log("Error fetching max_index, return default value")

            return max_index

    def update_index(self, end_index=10):
        # get lastest index
        q = self.session.query(func.max(bookScrape.id))
        start_index = 1
        try:
            start_index = int(q.one()[0])
        except Exception, e:
            self.log("Error querying start_index, return default value")

        for i in range(start_index+1, end_index+1):
            # http://stackoverflow.com/questions/36492731/sqlalchemy-bulk-insert-ignore-duplicate-entry
            try:
                self.session.add(bookScrape(
                    id=i,
                    httpcode=0
                ))
            except sqlalchemy.exc.IntegrityError, e:
                self.log("Ignoring duplicate entry: {err}".format(err=str(e)))

        self.session.commit()

    def start_requests(self):
        # start_url = "https://webcat.tpml.edu.tw/WebpacMobile/bookdetail.do"
        start_url = "https://webcat.tpml.edu.tw/webpac/bookDetail.do"

        q = self.session.query(bookScrape)

        # fetch all including errors
        q = q.filter(bookScrape.httpcode != 200)
        if not bool(self.config.get("general", "fetch_errors")):
            q = q.filter(bookScrape.httpcode == 0)

        unfetched = q.all()
        self.log("unfetched={}".format([(p.id, p.httpcode) for p in unfetched]))
        urls = map(
            lambda x: (x.id, "{start}?id={id}".format(start=start_url, id=x.id)),
            unfetched
        )
        for idx, url in urls:
            self.log(url)
            yield scrapy.Request(url=url, callback=lambda res, i=idx:self.parse(res, i))

    def parse(self, response, index):
        # self.log(response.body)

        _content = None
        _httpcode = -1
        try:
            # _content = Selector(response=response).xpath('//*[@id="page1"]/div[2]/div[1]').extract().pop()
            _content = Selector(response=response).xpath('//*[@id="detailViewMARC"]').extract().pop()
            _httpcode = response.status
        except IndexError, e:
            # TODO: handling non-exist indexes
            self.log("error parsing content: {err}".format(err=str(e)))

        # self.log(content)
        try:
            self.session.merge(bookScrape(
                id=index,
                httpcode=_httpcode,
                content=_content
            ))
            self.session.commit()
        except Exception, e:
            self.log("Error saving bookdata: {err}".format(err=str(e)))
            self.session.rollback()

