"""The generic one-factor expansion in _common: family-agnostic, so the future checkpoint
family reuses the validated mechanics instead of copy-pasting the read configurator."""

import dataclasses

import pytest

from gcsfs.tests.perf.subsystembenchmarks._common.config_loader import (
    OneFactorConfigurator,
)


@dataclasses.dataclass
class _P:
    name: str
    bucket_name: str
    framework: str
    sweep_axis: str
    color: str
    size: int

    def benchmark_name(self):
        return f"case-{self.color}-{self.size}"


class _C(OneFactorConfigurator):
    FRAMEWORK = "fake"
    PARAMS_CLASS = _P
    RUN_LEVEL_KEYS = ("size",)

    def shared_keys(self, scenario, common_config):
        return {"size": common_config.get("size", 1)}


_YAML = """
common:
  size: 3
  baseline:
    color: "red"
scenarios:
  - name: "s"
    variants:
      - {axis: "color", color: "blue"}
"""

_YAML_TWO_AXES = _YAML.replace(
    '- {axis: "color", color: "blue"}',
    '- {axis: "color", color: "blue"}\n      - {axis: "shade", color: "green"}',
)


def _configurator(tmp_path, text=_YAML):
    (tmp_path / "configs.yaml").write_text(text)
    return _C(str(tmp_path / "configs.py"))


def test_baseline_plus_variant_with_sweep_axis(tmp_path):
    cases = _configurator(tmp_path).generate_cases()
    assert [(c.name, c.sweep_axis) for c in cases] == [
        ("case-red-3", "baseline"),
        ("case-blue-3", "color"),
    ]
    assert all(c.framework == "fake" for c in cases)


def test_variant_without_axis_is_rejected(tmp_path):
    # `axis:` used to be a stripped comment; now it is data (the sweep_axis column), so a
    # variant that omits it must fail instead of silently publishing an unlabeled case.
    text = _YAML.replace('{axis: "color", color: "blue"}', '{color: "blue"}')
    with pytest.raises(ValueError, match="axis"):
        _configurator(tmp_path, text).generate_cases()


def test_duplicate_ids_are_rejected(tmp_path):
    text = _YAML.replace('{axis: "color", color: "blue"}', '{axis: "noop", size: 3}')
    # size is run-level here, so use a truly id-invisible no-op instead:
    text = text.replace('{axis: "noop", size: 3}', '{axis: "noop", color: "red"}')
    with pytest.raises(ValueError, match="share the benchmark id"):
        _configurator(tmp_path, text).generate_cases()


def test_run_level_key_override_is_rejected(tmp_path):
    text = _YAML.replace('{axis: "color", color: "blue"}', '{axis: "size", size: 9}')
    with pytest.raises(ValueError, match="run-level"):
        _configurator(tmp_path, text).generate_cases()


def test_unknown_key_is_rejected(tmp_path):
    text = _YAML.replace(
        '{axis: "color", color: "blue"}', '{axis: "color", colour: "blue"}'
    )
    with pytest.raises(ValueError, match="unknown key"):
        _configurator(tmp_path, text).generate_cases()


def test_baseline_clashing_with_shared_keys_is_rejected(tmp_path):
    text = _YAML.replace('color: "red"', 'color: "red"\n    size: 5')
    with pytest.raises(ValueError, match="size"):
        _configurator(tmp_path, text).generate_cases()


@pytest.mark.parametrize(
    ("requested", "expected"),
    [
        ("baseline", [("case-red-3", "baseline")]),
        ("color", [("case-red-3", "baseline"), ("case-blue-3", "color")]),
        (
            "shade color shade",
            [
                ("case-red-3", "baseline"),
                ("case-blue-3", "color"),
                ("case-green-3", "shade"),
            ],
        ),
    ],
)
def test_sweep_axis_filter_includes_baseline_and_preserves_case_order(
    tmp_path, monkeypatch, requested, expected
):
    monkeypatch.setenv("GCSFS_SUBSYSTEM_SWEEP_AXES", requested)
    cases = _configurator(tmp_path, _YAML_TWO_AXES).generate_cases()
    assert [(case.name, case.sweep_axis) for case in cases] == expected


def test_empty_sweep_axis_filter_runs_all_cases(tmp_path, monkeypatch):
    monkeypatch.delenv("GCSFS_SUBSYSTEM_SWEEP_AXES", raising=False)
    cases = _configurator(tmp_path, _YAML_TWO_AXES).generate_cases()
    assert [case.sweep_axis for case in cases] == ["baseline", "color", "shade"]


def test_unknown_sweep_axis_is_rejected(tmp_path, monkeypatch):
    monkeypatch.setenv("GCSFS_SUBSYSTEM_SWEEP_AXES", "workers")
    with pytest.raises(
        ValueError, match=r"unknown sweep axis.*workers.*baseline.*color"
    ):
        _configurator(tmp_path).generate_cases()
