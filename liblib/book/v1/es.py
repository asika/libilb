import hashlib
import datetime

from elasticsearch import Elasticsearch

def main(book_scrape_item, **kwargs):
    host = kwargs.get("host", "127.0.0.1")
    record = kwargs.get("record")

    es = Elasticsearch([
        {'host': host}
    ])

    fields = [
        'title',
        'author',
        'isbn',
        'publisher',
        'pubyear'
    ]
    doc = {
        'tpml_id': book_scrape_item.id,
        'ts': datetime.datetime.utcnow()
    }
    for f in fields:
        # http://stackoverflow.com/questions/3061/calling-a-function-of-a-module-from-a-string-with-the-functions-name-in-python
        doc[f] = getattr(record, f)()

    request_body = {
        "settings" : {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }
    }
 
    es.indices.create(index="tpml", body=request_body, ignore=400)
    es.index(index="tpml", doc_type="book_v1", body=doc, id=book_scrape_item.id)
