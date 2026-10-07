"""Runtime path defaults stay tied to the checkout, independent of process cwd."""

from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def backend_imports(monkeypatch):
    repository = Path(__file__).resolve().parents[2]
    monkeypatch.syspath_prepend(str(repository / "src" / "backend"))
    monkeypatch.delenv("AIOPS_DATA_DIR", raising=False)
    monkeypatch.delenv("AIOPS_MONITOR_DB", raising=False)
    monkeypatch.delenv("AIOPS_POLICY_REGISTRY_PATH", raising=False)
    monkeypatch.delenv("AIOPS_MONITOR_LEARNING_DIR", raising=False)


def test_default_paths_use_repository_data_not_src_data(monkeypatch, tmp_path):
    import data_paths

    monkeypatch.chdir(tmp_path)
    repository = Path(__file__).resolve().parents[2]
    assert data_paths.data_root() == repository / "data"
    assert data_paths.monitoring_data_dir() == repository / "data" / "monitoring"
    assert data_paths.policy_data_dir() == repository / "data" / "policy"
    assert data_paths.template_data_dir() == repository / "data" / "templates"
    assert data_paths.data_root() != repository / "src" / "data"


def test_absolute_and_relative_data_dir_are_cwd_independent(monkeypatch, tmp_path):
    import data_paths

    repository = Path(__file__).resolve().parents[2]
    absolute = tmp_path / "absolute-data"
    monkeypatch.setenv("AIOPS_DATA_DIR", str(absolute))
    monkeypatch.chdir(tmp_path)
    assert data_paths.data_root() == absolute

    monkeypatch.setenv("AIOPS_DATA_DIR", "./relative-data")
    assert data_paths.data_root() == repository / "relative-data"
    monkeypatch.chdir(tmp_path.parent)
    assert data_paths.data_root() == repository / "relative-data"


def test_monitoring_default_and_explicit_db_precedence(monkeypatch, tmp_path):
    from monitoring.runtime import database_path

    data = tmp_path / "runtime"
    monkeypatch.setenv("AIOPS_DATA_DIR", str(data))
    monkeypatch.chdir(tmp_path)
    assert database_path() == data / "monitoring" / "monitors.sqlite3"

    override = tmp_path / "other" / "monitor.sqlite3"
    monkeypatch.setenv("AIOPS_MONITOR_DB", str(override))
    assert database_path() == override


def test_parser_and_segmentation_policy_defaults_share_temp_db(monkeypatch, tmp_path):
    from parser_layer.policy.policy_registry import ParserPolicyRegistry
    from segmentation_layer.policy_registry import SegmentationPolicyRegistry

    data = tmp_path / "runtime"
    monkeypatch.setenv("AIOPS_DATA_DIR", str(data))
    expected = data / "policy" / "policy_registry.sqlite3"
    parser = ParserPolicyRegistry()
    segmentation = SegmentationPolicyRegistry()
    assert parser.db_path == expected
    assert Path(segmentation.db_path) == expected
    assert expected.is_file()


def test_policy_explicit_argument_precedes_environment(monkeypatch, tmp_path):
    from parser_layer.policy.policy_registry import ParserPolicyRegistry
    from segmentation_layer.policy_registry import SegmentationPolicyRegistry

    environment = tmp_path / "environment.sqlite3"
    explicit = tmp_path / "explicit.sqlite3"
    monkeypatch.setenv("AIOPS_DATA_DIR", str(tmp_path / "runtime"))
    monkeypatch.setenv("AIOPS_POLICY_REGISTRY_PATH", str(environment))
    assert ParserPolicyRegistry(db_path=explicit).db_path == explicit
    assert Path(SegmentationPolicyRegistry(db_path=str(explicit)).db_path) == explicit
    assert not environment.exists()


def test_policy_environment_override_precedes_data_dir(monkeypatch, tmp_path):
    from parser_layer.policy.policy_registry import ParserPolicyRegistry
    from segmentation_layer.policy_registry import SegmentationPolicyRegistry

    environment = tmp_path / "environment.sqlite3"
    data = tmp_path / "runtime"
    monkeypatch.setenv("AIOPS_DATA_DIR", str(data))
    monkeypatch.setenv("AIOPS_POLICY_REGISTRY_PATH", str(environment))
    assert ParserPolicyRegistry().db_path == environment
    assert Path(SegmentationPolicyRegistry().db_path) == environment
    assert not (data / "policy" / "policy_registry.sqlite3").exists()


def test_opensearch_policy_reader_uses_data_dir_without_creating_db(monkeypatch, tmp_path):
    import opensearch_application

    data = tmp_path / "runtime"
    monkeypatch.setenv("AIOPS_DATA_DIR", str(data))
    assert opensearch_application.list_verified_policies() == ()
    assert not (data / "policy" / "policy_registry.sqlite3").exists()


def test_template_defaults_and_explicit_paths(monkeypatch, tmp_path):
    import full_pipeline_v2

    data = tmp_path / "runtime"
    monkeypatch.setenv("AIOPS_DATA_DIR", str(data))
    monkeypatch.setattr(full_pipeline_v2, "SegmentationPipeline", lambda: object())
    monkeypatch.setattr(full_pipeline_v2, "ParserPipeline", lambda: object())
    monkeypatch.setattr(full_pipeline_v2, "DownstreamAIOpsPipeline", lambda *args, **kwargs: object())
    captured = []
    monkeypatch.setattr(
        full_pipeline_v2, "TemplatePipeline",
        lambda **kwargs: captured.append(kwargs) or object(),
    )

    full_pipeline_v2.FullAIOpsPipelineV2()
    assert captured[-1] == {
        "state_path": data / "templates" / "template_state_v4f.json",
        "candidate_state_path": data / "templates" / "template_drain_v4f.bin",
    }

    explicit_state = tmp_path / "custom-state.json"
    explicit_drain = tmp_path / "custom-drain.bin"
    full_pipeline_v2.FullAIOpsPipelineV2(
        template_state=explicit_state, drain_state=explicit_drain,
    )
    assert captured[-1] == {
        "state_path": explicit_state,
        "candidate_state_path": explicit_drain,
    }


def test_worker_learning_default_and_override_use_temp_dirs(monkeypatch, tmp_path):
    import full_pipeline_v2

    repository = Path(__file__).resolve().parents[2]
    monkeypatch.syspath_prepend(str(repository / "tools"))
    import monitor_worker

    captured = []
    monkeypatch.setattr(
        full_pipeline_v2, "FullAIOpsPipelineV2",
        lambda **kwargs: captured.append(kwargs) or object(),
    )
    monkeypatch.setenv("AIOPS_DATA_DIR", str(tmp_path / "runtime"))
    monitor_worker.pipeline.cache_clear()
    try:
        monitor_worker.pipeline()
        assert Path(captured[-1]["template_state"]) == (
            tmp_path / "runtime" / "monitoring" / "learning" / "templates.json"
        )
        assert Path(captured[-1]["drain_state"]) == (
            tmp_path / "runtime" / "monitoring" / "learning" / "drain.bin"
        )

        monitor_worker.pipeline.cache_clear()
        override = tmp_path / "worker-learning"
        monkeypatch.setenv("AIOPS_MONITOR_LEARNING_DIR", str(override))
        monitor_worker.pipeline()
        assert Path(captured[-1]["template_state"]) == override / "templates.json"
        assert Path(captured[-1]["drain_state"]) == override / "drain.bin"
    finally:
        monitor_worker.pipeline.cache_clear()
