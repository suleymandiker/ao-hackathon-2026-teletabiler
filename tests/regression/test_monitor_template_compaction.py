"""Pure synthetic checks for monitoring template text bounds."""
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'src' / 'backend'))

from monitoring.window_accumulator import (  # noqa: E402
    MAX_TEMPLATE_CHARS, TEMPLATE_TRUNCATION_MARKER, WindowAccumulator,
)


@pytest.mark.parametrize('length', [8192, 8193, 10613])
def test_analytical_template_representation_is_bounded_and_deterministic(length):
    template = 'HEAD' + 'x' * (length - 8) + 'TAIL'
    row = {'template_id': 'synthetic-template-id', 'template': template,
           'service_name': 'synthetic-service', 'attributes': {}}
    first = WindowAccumulator()
    second = WindowAccumulator()
    first.add(row)
    second.add(row)
    retained = first.patterns['synthetic-template-id'].template

    assert retained == second.patterns['synthetic-template-id'].template
    assert len(retained) <= MAX_TEMPLATE_CHARS
    assert first.pattern_metrics()[0]['original_template_chars'] == length
    if length == MAX_TEMPLATE_CHARS:
        assert retained == template
    else:
        assert retained.startswith('HEAD') and retained.endswith('TAIL')
        assert TEMPLATE_TRUNCATION_MARKER in retained
        assert first.pattern_metrics()[0]['template_truncated'] is True
