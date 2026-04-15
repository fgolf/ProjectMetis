"""
Sample classes for various data sources.

Usage:
    from metis.samples import DBSSample, DirectorySample, FilelistSample, DummySample
"""

import os
import re

from metis.Utils import setup_logger, cached

# Shared constants
DIS_CACHE_SECONDS = 5 * 60
if os.getenv("NOCACHE"):
    DIS_CACHE_SECONDS = 0

# Dataset name validation
_DATASET_PATTERN = re.compile(r'^/[A-Za-z0-9\-_.*?]+(/[A-Za-z0-9\-_.*?]+){0,2}$')

def _validate_dataset(dataset):
    """Validate that a dataset name looks like a CMS dataset path."""
    ds = dataset.replace(",all", "").strip()
    if not ds:
        raise ValueError("Empty dataset name")
    if not _DATASET_PATTERN.match(ds):
        raise ValueError("Invalid dataset format: {}".format(ds))
    return ds

# Public API
from metis.samples.base import Sample
from metis.samples.dbs import DBSSample
from metis.samples.directory import DirectorySample
from metis.samples.snt import SNTSample
from metis.samples.filelist import FilelistSample
from metis.samples.dummy import DummySample
