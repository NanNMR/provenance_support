#!/usr/bin/env python3
import argparse
import logging
import sys

import yaml
from postgresql_access import DatabaseDict, ReadOnlyCursor

from ptsupport import support_logger, TableMaker, set_database


class SchemaMaker:

    def __init__(self, config, exclude):
        self.db = DatabaseDict(dictionary=config['database'])
        self.exclude = exclude
        self.table_maker = TableMaker(config)
        print(f"excluding suffix {exclude}")

    def generate_schema(self,schema,output):
        with self.db.connect(application_name='provenance schema maker') as conn:
            with ReadOnlyCursor(conn) as curs:
                curs.execute("""select table_name from information_schema.tables 
                where table_type='BASE TABLE' and tables.table_schema = %s""",(schema,))
                tnames = [f"{schema}.{r[0]}" for r in curs.fetchall()]
                support_logger.debug(','.join(tnames))
        for name in tnames:
            if name.endswith(self.exclude):
                print(f"excluding {name}")
                continue
            self.table_maker.createTable(name,output)




def main():
    logging.basicConfig()
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('schema', help="schema")
    parser.add_argument('output', help="File to write to")
    parser.add_argument('-l', '--loglevel', default='WARNING', help="Python logging level")
    parser.add_argument('--yaml', default='local.yaml', help="YAML")
    parser.add_argument('--exclude',default='_history',help="Exclude tables ending with this")
    parser.add_argument('--database',help="Use this database instead of one in yaml file")

    args = parser.parse_args()
    support_logger.setLevel(getattr(logging, args.loglevel))
    with open(args.yaml) as f:
        config = yaml.safe_load((f))
    set_database(config,args)
    sm = SchemaMaker(config,args.exclude)
    with open(args.output, 'w') as f:
        sm.generate_schema(args.schema, f)


if __name__ == "__main__":
    main()
