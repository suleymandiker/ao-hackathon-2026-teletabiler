# -*- coding: utf-8 -*-
import json
import os
from pathlib import Path

from drain3 import TemplateMiner
from drain3.file_persistence import FilePersistence
from drain3.template_miner_config import TemplateMinerConfig


class DrainCandidateMiner:
    """Thin Drain3 adapter. Its state contains candidates, never final decisions."""

    def __init__(self, state_path=None, similarity_threshold=0.5, max_clusters=50_000):
        if not 0.0 <= similarity_threshold <= 1.0:
            raise ValueError("similarity_threshold must be between 0 and 1")
        if max_clusters < 1:
            raise ValueError("max_clusters must be positive")

        config = TemplateMinerConfig()
        config.drain_sim_th = similarity_threshold
        config.drain_max_clusters = max_clusters
        config.masking_instructions = []
        config.parametrize_numeric_tokens = False
        config.snapshot_interval_minutes = 1

        self.state_path = Path(state_path) if state_path else None
        persistence = None
        if self.state_path:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            persistence = FilePersistence(str(self.state_path))
        self._miner = TemplateMiner(persistence_handler=persistence, config=config)

    @property
    def cluster_count(self):
        return len(self._miner.drain.clusters)

    def propose(self, deterministic_template):
        result = self._miner.add_log_message(deterministic_template)
        return {
            "template": result["template_mined"],
            "cluster_id": result["cluster_id"],
            "cluster_size": result["cluster_size"],
            "change_type": result["change_type"],
        }

    def save(self):
        if not self.state_path:
            return False
        self._miner.save_state("explicit-save")
        return self.state_path.exists()


class ValidatedTemplateRegistry:
    """Authoritative registry containing validator-approved templates only."""

    STATE_VERSION = 1

    def __init__(self, state_path=None, max_templates=50_000):
        if max_templates < 1:
            raise ValueError("max_templates must be positive")
        self.state_path = Path(state_path) if state_path else None
        self.max_templates = max_templates
        self._templates = {}
        if self.state_path:
            self._load()

    @property
    def template_count(self):
        return len(self._templates)

    def get(self, deterministic_template):
        return self._templates.get(deterministic_template)

    def promote(self, deterministic_template, template):
        current = self._templates.get(deterministic_template)
        if current is not None:
            return current, False
        if self.template_count >= self.max_templates:
            return None, False
        self._templates[deterministic_template] = template
        return template, True

    def save(self):
        if not self.state_path:
            return False
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"version": self.STATE_VERSION, "templates": self._templates}
        temporary = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        os.replace(temporary, self.state_path)
        return True

    def _load(self):
        if not self.state_path.exists():
            return
        payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        if payload.get("version") != self.STATE_VERSION:
            raise ValueError("unsupported validated template state version")
        templates = payload.get("templates")
        if not isinstance(templates, dict):
            raise ValueError("invalid validated template state")
        if len(templates) > self.max_templates:
            raise ValueError("validated template state exceeds max_templates")
        if not all(isinstance(key, str) and isinstance(value, str) for key, value in templates.items()):
            raise ValueError("invalid validated template entry")
        self._templates = templates
