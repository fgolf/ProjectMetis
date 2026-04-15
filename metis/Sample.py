"""
Backwards compatibility shim — imports from metis.samples package.

Use `from metis.samples import DBSSample` for new code.
"""
from metis.samples import (
    Sample, DBSSample, DirectorySample, SNTSample,
    FilelistSample, DummySample,
    _validate_dataset, DIS_CACHE_SECONDS,
)
