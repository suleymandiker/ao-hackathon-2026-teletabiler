"""Stable source-reference identity shared by acquisition and ownership."""

import hashlib
import json


def reference_key(record):
    reference = record.source_reference
    # A document update does not create a second physical source record.
    return hashlib.sha256(json.dumps((reference.source_scope, reference.source_partition,
                                     reference.record_id), separators=(',', ':')).encode()).hexdigest()
