#!/usr/bin/env python3
import argparse
import io
import logging
import os
import sys

import yaml

from ptsupport import support_logger

os.environ['NO_PROVENANCE_TRACK_LOG'] = "1"
from postgresql_access import DatabaseDict

_PROV_COLUMNS = (
    'provenance_timestamp timestamp with time zone not null default now()',
    'provenance_user text not null',
    'provenance_event integer not null'
)
_ROW_TRIGGER = """CREATE TRIGGER {trigger} 
BEFORE
INSERT or UPDATE or DELETE  
ON {fqtn} FOR EACH ROW EXECUTE PROCEDURE public.record_provenance() ;
"""
_TRUNCATE_TRIGGER = """CREATE TRIGGER {trigger} 
BEFORE
TRUNCATE
ON {fqtn} EXECUTE PROCEDURE public.dont_truncate_provenance();
"""


class TableMaker:

    def __init__(self, config):
        self.db = DatabaseDict(dictionary=config['database'])

    def _process_column(self, iraw: str) -> str | None:
        #        raw = iraw.replace('timestamp with time zone', 'timestamptz')
        raw = iraw.strip()
        if raw.startswith('CONSTRAINT'):
            return None
        nspot = raw.find('NULL')
        if nspot > 0:
            return raw[:nspot + 4].strip()
        gspot = raw.find('GENERATED')
        if gspot > 0:
            return raw[:gspot].strip() + ' NOT NULL'
        raise ValueError(f"Unparsed {raw}")

    def _create_triggers(self, schema, table, buffer)->None:
        """Add trigger statements to buffer"""
        row_trigger = f"provenance_trigger_{schema}_{table}"
        fqtn = f"{schema}.{table}"
        print(f"DROP TRIGGER IF EXISTS {row_trigger} on {fqtn};", file=buffer)
        print(_ROW_TRIGGER.format(trigger=row_trigger,fqtn=fqtn),file=buffer)
        trunc_trigger = f"provenance_truncate_trigger_{schema}_{table}"
        print(f"DROP TRIGGER IF EXISTS {trunc_trigger} on {fqtn};", file=buffer)
        print(_TRUNCATE_TRIGGER.format(trigger=trunc_trigger,fqtn=fqtn),file=buffer)

    def createTable(self, name, out)->None:
        if '.' in name:
            schema, table = name.split('.')
        else:
            schema = 'public'
            table = name
        cdefs = ['provenance_track_id integer not null generated always as identity primary key']
        with self.db.connect(application_name='create provenance name') as conn:
            with conn.cursor() as curs:
                curs.execute('select pg_get_tabledef(%s,%s,false)', (schema, table))
                definition = curs.fetchone()[0]
                parts = [c for c in definition.split('\n') if c != '']
                cols = []
                for p in parts[1:-2]:
                    if p.startswith('  CONSTRAINT'):
                        break
                    cols.append(p)
                for c in cols:
                    if (cdef := self._process_column(c)) is not None:
                        cdefs.append(cdef)
                cdefs.extend(_PROV_COLUMNS)
                support_logger.info(cdefs)
        buffer = io.StringIO()
        print(f"Create table provenance.{schema}_{table} (", file=buffer)
        idented = ['   ' + c for c in cdefs]
        print(',\n'.join(idented), file=buffer)
        print(');', file=buffer)
        self._create_triggers(schema,table,buffer)
        print(buffer.getvalue(), file=out)


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-l', '--loglevel', default='INFO', help="Python logging level")
    parser.add_argument('--yaml', default='local.yaml', help="YAML")
    parser.add_argument('table_name', help="table name [schema.table]")
    parser.add_argument('-o', '--output', help="File to write to")

    args = parser.parse_args()
    support_logger.setLevel(getattr(logging, args.loglevel))
    with open(args.yaml) as f:
        config = yaml.safe_load((f))
    tm = TableMaker(config)
    if args.output:
        with open(args.output, 'w') as f:
            tm.createTable(args.table_name, f)
    else:
        tm.createTable(args.table_name, sys.stdout)


if __name__ == "__main__":
    main()
