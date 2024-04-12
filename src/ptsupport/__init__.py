import importlib.metadata
import logging
import sys

VERSION = importlib.metadata.version("provenance_support")
support_logger = logging.getLogger("ptsupport")


def set_database(config,args)->None:
    if args.database:
        config['database']['database'] = args.database
    print(f"Parsing {config['database']['database']}",file=sys.stderr)
from ptsupport.createtable import TableMaker
