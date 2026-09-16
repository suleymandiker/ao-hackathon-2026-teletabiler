# -*- coding: utf-8 -*-
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TemplateResult:
    """Small, stable output contract of the Template Layer."""

    template_id: str
    template: str
    reliable: bool
