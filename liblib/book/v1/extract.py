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
            subf_split = line.strip().split("|")
            # print line
            # print subf_split

            for subf in subf_split:
                if subf:
                    # |aA14 C12 D12 L41 H16 K13 C41 K41 D41
                    subfields.append(subf[0])
                    if len(subf) > 1:
                        subfields.append(subf[1:])
                    else:
                        subfields.append("")

    return subfields

def tpml_marcfix(record):
    def _get_field(record, tag, ind):
        fields = record.get_fields(tag)

        for f in fields:
            if f[ind] is not None:
                return f

    if record.author() is None:
        name_fields = record.get_fields('700')
        _set = False

        for f in name_fields:
            if f['e'] is None or f['e'] == u"著":
                # copy author information from 700 to 100 (main entry)
                record.add_field(
                    Field(tag='100', indicators=f.indicators, subfields=f.subfields)
                )
                _set = True
                break

        if not _set:
            try:
                f = record['245']['c'] 
                if f is not None:
                    record.add_field(
                        Field(tag='100', indicators=f.indicators, subfields=f.subfields)
                    )
                    _set = True
            except IndexError, e:
                pass

        if not _set:
            try:
                f = record['710']['a'] 
                if f is not None:
                    record.add_field(
                        Field(tag='100', indicators=f.indicators, subfields=f.subfields)
                    )
                    _set = True
            except IndexError, e:
                pass

    if record.publisher() is None:
        _set = False

        f = _get_field(record, '260', 'b')
        if f is not None:
            record.add_field(
                Field(tag='260', indicators=f.indicators, subfields=f.subfields)
            )
            _set = True


    return record

def main(item, **kwargs):
    raw_tpml_details = item.content

    # fix incomplete html structure
    #fixed, _ = re.subn("<\/tr>\n[  ]*\n[  ]*<table>", "<\/tr>\n", raw_tpml_details)
    fixed, _ = re.subn("</tr>\r\n[\t ]*\r\n[\t ]*<table>", "</tr>\r\n", raw_tpml_details)

    record = Record()

    sel = Selector(text=fixed)
    sel_marc_entries = sel.xpath('//*[@id="detailViewMARC"]/table[1]/tr')

    my_marc = {}

    last_tag = None
    for sel_entry in sel_marc_entries:
        try:
            tag, ind, subf = map(
                lambda x:x.replace("<td>", "").replace("</td>", ""),
                sel_entry.xpath('td').extract() 
            )
            if last_tag is None:
                last_tag = tag
            elif last_tag != tag:
                # print "insert last_tag=", my_marc[last_tag]
                record.add_field(
                    Field(
                        tag=last_tag,
                        indicators=_parse_ind(ind),
                        subfields=my_marc[last_tag]
                    )
                )
            last_tag = tag

        except ValueError, e:
            print "Error extract: {err}".format(err=str(e))
            print "="*40
            print fixed 
            print "="*40

        # record.add_field(
        #     Field(
        #         tag=tag,
        #         indicators=_parse_ind(ind),
        #         subfields=_parse_subf(subf)
        #     )
        # )
        if tag not in my_marc.keys():
            my_marc[tag] = []

        my_marc[tag] += _parse_subf(subf)

    # add last tag
    record.add_field(
        Field(
            tag=last_tag,
            indicators=_parse_ind(ind),
            subfields=my_marc[last_tag]
        )
    )
    record = tpml_marcfix(record)

    return record 
