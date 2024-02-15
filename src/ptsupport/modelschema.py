#!/usr/bin/env python3
import argparse
import logging
import sys

import yaml
from postgresql_access import DatabaseDict, ReadOnlyCursor

from ptsupport import support_logger, TableMaker


class SchemaMaker:

    def __init__(self, config):
        self.db = DatabaseDict(dictionary=config['database'])
        self.table_maker = TableMaker(config)

    def generate_schema(self,schema,output):
        with self.db.connect(application_name='provenance schema maker') as conn:
            with ReadOnlyCursor(conn) as curs:
                curs.execute("""select table_name from information_schema.tables 
                where table_type='BASE TABLE' and tables.table_schema = %s""",(schema,))
                tnames = [f"{schema}.{r[0]}" for r in curs.fetchall()]
                support_logger.debug(','.join(tnames))
        for name in tnames:
            self.table_maker.createTable(name,output)




def main():
    logging.basicConfig()
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-l', '--loglevel', default='INFO', help="Python logging level")
    parser.add_argument('--yaml', default='local.yaml', help="YAML")
    parser.add_argument('schema', help="schema")
    parser.add_argument('-o', '--output', help="File to write to")

    args = parser.parse_args()
    support_logger.setLevel(getattr(logging, args.loglevel))
    with open(args.yaml) as f:
        config = yaml.safe_load((f))
    sm = SchemaMaker(config)
    if args.output:
        with open(args.output, 'w') as f:
            sm.generate_schema(args.schema, f)
    else:
        sm.generate_schema(args.schema, sys.stdout)


if __name__ == "__main__":
    main()
