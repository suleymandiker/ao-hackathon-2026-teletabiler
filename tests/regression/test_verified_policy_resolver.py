"""Automatic selection is bounded, deterministic and uses only verified state."""
from dataclasses import replace

import pytest

from test_opensearch_application import app, local_catalog_dir, install, request, source_page


HEADER = r'^2026-10-04 '


def records(texts):
    base = source_page(1).records[0]
    return tuple(replace(base, raw_text=text) for text in texts)


def verified(app, signature, regex=HEADER):
    from segmentation_layer.contracts import SegmentationPolicy
    return app.module.VerifiedPolicy(signature, SegmentationPolicy(signature, regex, 'test-verified'))


def test_deterministic_selection_preserves_snapshot_and_raw_records(app):
    sample = records(['2026-10-04 09:00:00 ERROR first', '    traceback', '', '2026-10-04 09:00:01 INFO next'])
    correct = verified(app, 'a')
    wrong = verified(app, 'b', '^OTHER ')
    resolver = app.module.VerifiedPolicyResolver()
    one = resolver.resolve(sample, (wrong, correct))
    two = resolver.resolve(sample, (correct, wrong))
    assert one == two and one.snapshot is correct.snapshot
    assert one.matched_headers == 2 and one.compatible_streams == 1
    assert sample[1].raw_text == '    traceback' and sample[2].raw_text == ''
    assert HEADER not in repr(one)


@pytest.mark.parametrize('regex,texts', [
    ('^OTHER ', ['2026-10-04 09:00:00 ERROR first', '2026-10-04 09:00:01 INFO next']),
    (HEADER, ['    2026-10-04 one', '    2026-10-04 two']),
    (HEADER, ['2026-10-04 09:00:00 INFO single']),
    ('^.*', ['one', 'two']),
    (HEADER, ['2026-10-04 09:00:00 INFO first', '2026-10-04 09:00:01 INFO second', '2026-10-05 09:00:00 INFO missed']),
])
def test_weak_incompatible_or_unsafe_policy_is_not_selected(app, regex, texts):
    from verified_policy_resolver import PolicyResolutionError
    with pytest.raises(PolicyResolutionError, match='no_policy'):
        app.module.VerifiedPolicyResolver().resolve(records(texts), (verified(app, 'a', regex),))


def test_equivalent_boundaries_are_ambiguous_regardless_of_catalog_order(app):
    from verified_policy_resolver import PolicyResolutionError
    policies = (verified(app, 'a'), verified(app, 'b', '^2026-10-04 09:'))
    sample = records(['2026-10-04 09:00:00 INFO first', '2026-10-04 09:00:01 INFO next'])
    for ordering in (policies, policies[::-1]):
        with pytest.raises(PolicyResolutionError, match='ambiguous_policy'):
            app.module.VerifiedPolicyResolver().resolve(sample, ordering)


def test_every_sampled_stream_needs_compatible_evidence(app):
    from verified_policy_resolver import PolicyResolutionError
    sample = records(['2026-10-04 09:00:00 INFO first', '2026-10-04 09:00:01 INFO next', 'unrecognized format'])
    sample = (*sample[:2], replace(sample[2], stream_identity=replace(sample[2].stream_identity, pod_instance='another-pod')))
    with pytest.raises(PolicyResolutionError, match='no_policy'):
        app.module.VerifiedPolicyResolver().resolve(sample, (verified(app, 'a'),))


def test_resolver_consumes_only_bounded_initial_records(app):
    from verified_policy_resolver import MAX_SAMPLE_RECORDS
    row = records(['2026-10-04 09:00:00 INFO repeated'])[0]
    consumed = []

    def supplied():
        for _ in range(MAX_SAMPLE_RECORDS):
            consumed.append(True)
            yield row
        pytest.fail('Resolver consumed beyond its sample budget')

    selection = app.module.VerifiedPolicyResolver().resolve(supplied(), (verified(app, 'a'),))
    assert selection.sampled_records == len(consumed) == MAX_SAMPLE_RECORDS


def test_oversized_samples_and_catalogs_fail_without_truncating_content(app):
    from verified_policy_resolver import MAX_SAMPLE_CHARACTERS, MAX_VERIFIED_POLICIES, PolicyResolutionError
    resolver = app.module.VerifiedPolicyResolver()
    chosen = verified(app, 'a')
    with pytest.raises(PolicyResolutionError):
        resolver.resolve(records(['x' * (MAX_SAMPLE_CHARACTERS + 1)]), (chosen,))
    with pytest.raises(PolicyResolutionError):
        resolver.resolve(records(['2026-10-04 one', '2026-10-04 two']), (chosen,) * (MAX_VERIFIED_POLICIES + 1))


def test_automatic_analysis_replays_original_pages_with_no_registry_mutation_or_discovery(app, monkeypatch):
    import segmentation_layer.header_discovery as discovery
    monkeypatch.setattr(discovery.HeaderDiscovery, 'discover', app.forbidden)
    app.seed([('0123456789abcdef01234567', HEADER)])
    before = app.database.read_bytes()
    first = replace(source_page(1), records=records(['2026-10-04 09:00:00 INFO first', '2026-10-04 09:00:01 INFO next']))
    second = replace(source_page(2, exhausted=True), records=records(['2026-10-04 09:00:02 ERROR last']))
    fake = install(app, monkeypatch, [first, second])
    result = app.module.run_analysis(fake.factory, request(app), automatic=True)
    assert fake.feeds[0] is first and fake.feeds[1] is second
    assert [call['cursor'] for call in fake.calls] == [None, first.next_cursor]
    assert all(item is fake.policies[0] for item in fake.policies)
    assert fake.policies[0].policy_id == '0123456789abcdef01234567'
    assert app.database.read_bytes() == before
    assert result['source_summary']['policy_selection']['mode'] == 'automatic'
    assert HEADER not in str(result)


@pytest.mark.parametrize('rows,code', [([], 'no_policy'), ([('wrong', '^OTHER ')], 'no_policy'),
                                     ([('a', HEADER), ('b', '^2026-10-04 09:')], 'ambiguous_policy')])
def test_automatic_selection_failure_never_constructs_pipeline(app, monkeypatch, rows, code):
    app.seed(rows)
    page = replace(source_page(1, exhausted=True), records=records(['2026-10-04 09:00:00 INFO one', '2026-10-04 09:00:01 INFO two']))
    fake = install(app, monkeypatch, [page])
    with pytest.raises(app.module.ApplicationError) as error:
        app.module.run_analysis(app.forbidden, request(app), automatic=True)
    assert error.value.code == code
    assert len(fake.calls) == (1 if rows else 0)
