import importlib.metadata
import logging

VERSION = importlib.metadata.version("provenance_support")
support_logger = logging.getLogger("ptsupport")
from ptsupport.createtable import TableMaker