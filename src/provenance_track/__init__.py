
import importlib.metadata 
import logging
provenance_track_logger = logging.getLogger(__name__)
from provenance_track.explore import explore

__version__ =  importlib.metadata.version('provenance_track') 

