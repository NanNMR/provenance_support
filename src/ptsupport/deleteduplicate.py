#!/usr/bin/env python3
import argparse
import logging
from dataclasses import dataclass

import psycopg2.extras
import yaml
from postgresql_access import DatabaseDict, ReadOnlyCursor

from ptsupport import support_logger, set_database


@dataclass(frozen=True)
class ColumnInfo:
    column: str
    data_type: str
    udt_name: str


PROV_COLUMNS = ('provenance_track_id', 'provenance_user', 'provenance_event')


class ProvenanceDedup:

    def __init__(self, config):
        self.db = DatabaseDict(dictionary=config['database'])

    def __enter__(self):
        self.conn = self.db.connect(application_name='provenance deduplication')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.rollback()
        self.conn.close()

    def remove_dups(self):
        with ReadOnlyCursor(self.conn) as self.curs:
            self._parse_provenance()

    def _parse_provenance(self):
        self.curs.execute("""select table_name from information_schema.tables 
        where table_type='BASE TABLE' and tables.table_schema = 'provenance'""")
        tnames = [r[0] for r in self.curs.fetchall()]
        support_logger.debug(','.join(tnames))
        for n in tnames:
            try:
                if not n.startswith('provenance_'):
                    self._dedup(n)
            except Exception:
                support_logger.exception(f"compare {n}")

    @staticmethod
    def _differ(prior, result):
        for k, v in prior.items():
            if result[k] != v:
                return True
        return False

    def _keys_for(self, tablename):
        schema, table = tablename.split('_', maxsplit=1)
        return self._primary_keys(schema, table)

    def _primary_keys(self, schema, table):
        query = """SELECT kcu.column_name
            FROM information_schema.table_constraints AS tc 
            JOIN information_schema.key_column_usage AS kcu 
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
              AND tc.table_name = kcu.table_name
                WHERE tc.table_schema = %s 
              AND tc.table_name = %s
              AND tc.constraint_type = 'PRIMARY KEY' """
        with self.conn.cursor() as curs:
            q = curs.mogrify(query, (schema, table))
            curs.execute(q)
            if curs.rowcount < 1:
                with open('/tmp/debugsql', 'w') as f:
                    print(q.decode(), file=f)
                raise ValueError(f"no key info {schema}.{table}")
            curs.execute(q)
            keys = set(r[0] for r in curs.fetchall())
            return keys

    def _dedup(self, tablename):
        keys = self._keys_for(tablename)
        with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
            q = f"select * from provenance.{tablename} order by {','.join(keys)}, provenance_timestamp"
            support_logger.info(q)
            curs.execute(q)
            prior = None

            for data in curs:
                if data['provenance_event'] != 1:
                    prior = None
                    continue
                if prior is None:
                    prior = {k: v for k, v in data.items() if k not in PROV_COLUMNS}
                    continue
                if self._differ(prior, data):
                    prior = {k: v for k, v in data.items() if k not in PROV_COLUMNS}
                    continue
                if support_logger.isEnabledFor(logging.DEBUG):
                    for k, v in prior.items():
                        support_logger.debug(f'{k}: {v} {data[k]}')
                    support_logger.debug(tablename)

                try:
                    id = data['provenance_track_id']
                    support_logger.info(f"duplicate {tablename} {id}")
                except KeyError:
                    support_logger.exception(tablename)

    def _columns(self, schema, table):
        self.curs.execute("""select column_name
            from information_schema.columns 
            where table_schema = %s and table_name = %s""", (schema, table))
        rval = set()
        for col in [r[0] for r in self.curs.fetchall()]:
            if not col.startswith('provenance_'):
                rval.add(col)
        return rval


def main():
    logging.basicConfig()
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-l', '--loglevel', default='INFO', help="Python logging level")
    parser.add_argument('--yaml', default='local.yaml', help="YAML")
    #    parser.add_argument('-o', '--output', help="File to write to")
    parser.add_argument('--database', help="Use this database instead of one in yaml file")

    args = parser.parse_args()
    support_logger.setLevel(getattr(logging, args.loglevel))
    with open(args.yaml) as f:
        config = yaml.safe_load((f))
    set_database(config, args)
    with ProvenanceDedup(config) as dd:
        dd.remove_dups()


if __name__ == "__main__":
    main()
