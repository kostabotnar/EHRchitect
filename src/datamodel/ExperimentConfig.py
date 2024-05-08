from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from mashumaro.mixins.json import DataClassJSONMixin


@dataclass(frozen=True)
class ExperimentTimeFrame(DataClassJSONMixin):
    min_date: Optional[str] = None
    max_date: Optional[str] = None


@dataclass(frozen=True)
class AttributeExperimentEvent(DataClassJSONMixin):
    events: list[ExperimentEvent]
    period: Optional[ExperimentTimeInterval] = None


@dataclass(frozen=True)
class ExperimentEvent(DataClassJSONMixin):
    id: str
    category: str
    name: str = None
    codes: list = field(default_factory=list)
    num_value: str = None
    text_value: str = None
    negation: bool = False
    include_subcodes: bool = False
    exclude: AttributeExperimentEvent = None
    having: AttributeExperimentEvent = None
    period: ExperimentTimeInterval = None


@dataclass(frozen=True)
class ExperimentTimeInterval(DataClassJSONMixin):
    min_t: int
    max_t: int
    unit: str = 'day'

    def get_min_t_days(self):
        if self.unit == 'month':
            min_t = self.min_t * 30
        elif self.unit == 'year':
            min_t = self.min_t * 365
        else:
            min_t = self.min_t
        return min_t

    def get_max_t_days(self):
        if self.unit == 'month':
            max_t = self.max_t * 30
        elif self.unit == 'year':
            max_t = self.max_t * 365
        else:
            max_t = self.max_t
        return max_t


class MatchMode(Enum):
    first_match = 'first_match'
    all_matches = 'all_matches'


@dataclass(frozen=True)
class ExperimentLevel(DataClassJSONMixin):
    level: int
    name: str = None
    period: ExperimentTimeInterval = None
    events: list[ExperimentEvent] = field(default_factory=list[ExperimentEvent])
    match_mode: MatchMode = MatchMode.all_matches


@dataclass
class ExperimentConfig(DataClassJSONMixin):
    name: str = None
    levels: list[ExperimentLevel] = field(default_factory=list[ExperimentLevel])
    comorbidity_scores: list[str] = field(default_factory=list[str])
    outcome_dir: str = None
    time_frame: Optional[ExperimentTimeFrame] = None

    def __post_init__(self):
        self.outcome_dir = None if self.name is None else self.name.replace('/', '_')

    def get_level_by_number(self, number: int) -> ExperimentLevel:
        return self.levels[number]
