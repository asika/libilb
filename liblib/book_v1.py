# -*- coding: utf-8 -*-
import re

from scrapy.selector import Selector
import sqlalchemy
from pymarc import Record, Field

def _parse_ind(ind):
    return ['0', '0']

def _parse_subf(subf):
    # skip incomplete structure
    # <td style="word-break: break-all; width: 500px">
    lines = map(unicode.strip, subf.split("\n"))
    if lines[0].startswith("<td style="):
        lines = lines[1:]

    subfields = []
    for line in lines:
        if line.startswith("|"):
            # |aA14 C12 D12 L41 H16 K13 C41 K41 D41
            subfields.append(line[1])
            subfields.append(line[2:])
        else:
            subfields.append('')
            subfields.append(line)

    return subfields

def extract(raw_tpml_details):
    # fix incomplete html structure
    fixed, _ = re.subn("<\/tr>\n[  ]*\n[  ]*<table>", "<\/tr>\n", raw_tpml_details)

    record = Record()

    sel = Selector(text=fixed)
    sel_marc_entries = sel.xpath('//*[@id="detailViewMARC"]/table[1]/tr')
    for sel_entry in sel_marc_entries:
        tag, ind, subf = map(
            lambda x:x.replace("<td>", "").replace("</td>", ""),
            sel_entry.xpath('td').extract() 
        )

        record.add_field(
            Field(
                tag=tag,
                indicators=_parse_ind(ind),
                subfields=_parse_subf(subf)
            )
        )

    return record 
