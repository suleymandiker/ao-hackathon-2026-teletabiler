"""Explicit source-profile index strategies; no discovery or local-clock use."""
from datetime import datetime, timedelta, timezone
import re


def validate_index_strategy(expression: str, strategy: str) -> None:
    if strategy not in ('literal', 'daily_utc'):
        raise ValueError('index_strategy must be literal or daily_utc')
    if strategy == 'daily_utc' and (type(expression) is not str or
                                   re.fullmatch(r'[a-z0-9][a-z0-9._-]*\*', expression) is None):
        raise ValueError('daily_utc requires one explicit <base>* index pattern')


def resolve_index_expression(expression: str, start: datetime, end: datetime, *, strategy: str = 'literal') -> str:
    """Resolve the UTC dates touched by the actual half-open acquisition bounds.

    daily_utc attests that this profile uses <base>-YYYY.MM.DD indices. Other
    profiles retain the literal expression, including aliases and wildcards.
    The caller owns interval/page bounds; no index-list request is performed.
    """
    validate_index_strategy(expression, strategy)
    for endpoint in (start, end):
        if not isinstance(endpoint, datetime) or endpoint.utcoffset() is None:
            raise ValueError('Index resolution requires aware acquisition bounds')
    start, end = start.astimezone(timezone.utc), end.astimezone(timezone.utc)
    if start >= end:
        raise ValueError('Acquisition interval must satisfy start < end')
    if strategy == 'literal':
        return expression
    first = start.date()
    last = (end - timedelta(microseconds=1)).date()
    base = expression[:-1]
    return ','.join(f'{base}-{first + timedelta(days=offset):%Y.%m.%d}'
                    for offset in range((last - first).days + 1))
