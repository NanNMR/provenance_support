#!/usr/bin/env python3
import argparse
import logging
from dataclasses import dataclass

import yaml
from postgresql_access import DatabaseDict, ReadOnlyCursor

from ptsupport import support_logger, TableMaker, set_database
@dataclass(frozen=True)
class ColumnInfo:
    column : str
    data_type :str
    udt_name :str


column: str


class ProvenanceAudit:

    def __init__(self, config):
        self.db = DatabaseDict(dictionary=config['database'])


    def audit(self):
        with self.db.connect(application_name='provenance schema maker') as conn:
            with ReadOnlyCursor(conn) as self.curs:

                self._parse_provenance()

    def _parse_provenance(self):
        self.curs.execute("""select table_name from information_schema.tables 
        where table_type='BASE TABLE' and tables.table_schema = 'provenance'""")
        tnames = [r[0] for r in self.curs.fetchall()]
        support_logger.debug(','.join(tnames))
        for n in tnames:
            try:
                if not n.startswith('provenance_'):
                    self._compare(n)
            except Exception:
                support_logger.exception(f"compare {n}")

    def _compare(self,tablename):
        schema, table = tablename.split('_',maxsplit=1)
        prov_columns = self._column_info('provenance',tablename)
        schema_columns = self._column_info(schema,table)
        same = prov_columns == schema_columns
        if not same:
            common = prov_columns & schema_columns
            only_prov = prov_columns - common
            only_schema = schema_columns - common
            print(f"{schema} {table} {only_prov} {only_schema}")



    def _column_info(self,schema,table):
        self.curs.execute("""select column_name,data_type,udt_name 
            from information_schema.columns 
            where table_schema = %s and table_name = %s""",(schema,table) )
        rval = set()
        for r in self.curs.fetchall():
            ci = ColumnInfo(*r)
            if not ci.column.startswith('provenance_'):
                rval.add(ci)
        return rval






def main():
    logging.basicConfig()
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-l', '--loglevel', default='INFO', help="Python logging level")
    parser.add_argument('--yaml', default='local.yaml', help="YAML")
#    parser.add_argument('-o', '--output', help="File to write to")
    parser.add_argument('--database',help="Use this database instead of one in yaml file")

    args = parser.parse_args()
    support_logger.setLevel(getattr(logging, args.loglevel))
    with open(args.yaml) as f:
        config = yaml.safe_load((f))
    set_database(config,args)
    auditor= ProvenanceAudit(config)
    auditor.audit()


if __name__ == "__main__":
    main()
