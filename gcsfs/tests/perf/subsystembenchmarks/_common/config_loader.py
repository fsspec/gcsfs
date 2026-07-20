"""Self-contained configs.yaml loader (vendored from microbenchmarks, no shared imports).

Deliberately duplicates the microbenchmarks configurator instead of importing it (or the
repo-wide test conftest/settings) so subsystembenchmarks can evolve without dragging the
micro suite along.
"""

import logging
import os

import yaml


class BaseBenchmarkConfigurator:
    def __init__(self, module_file):
        self.config_path = os.path.join(os.path.dirname(module_file), "configs.yaml")

    def _load_config(self):
        with open(self.config_path, "r") as f:
            config = yaml.safe_load(f)

        common = config["common"]
        scenarios = config["scenarios"]

        benchmark_filter = os.environ.get("GCSFS_BENCHMARK_FILTER", "")
        if benchmark_filter:
            filter_names = [
                name.strip().lower() for name in benchmark_filter.split(",")
            ]
            scenarios = [s for s in scenarios if s["name"].lower() in filter_names]

        return common, scenarios

    def generate_cases(self):
        common_config, scenarios = self._load_config()
        all_cases = []

        for scenario in scenarios:
            cases = self.build_cases(scenario, common_config)
            all_cases.extend(cases)

        if all_cases:
            logging.info(
                f"Benchmark cases to be triggered: {', '.join([case.name for case in all_cases])}"
            )
        return all_cases

    def build_cases(self, scenario, common_config):
        """
        Abstract method to be implemented by subclasses.
        Should return a list of BenchmarkParameters objects.
        """
        raise NotImplementedError


class OneFactorConfigurator(BaseBenchmarkConfigurator):
    """Family-agnostic one-factor-at-a-time expansion: a baseline plus one variant per axis.

    A benchmark family (read, later checkpoint) subclasses this once, pinning:
      - FRAMEWORK / PARAMS_CLASS  -- per engine group, like before;
      - RUN_LEVEL_KEYS            -- keys owned by the run env that yaml must never set;
      - shared_keys()             -- the family's cross-case keys (rounds, batch sizes, ...);
      - validate_case()           -- optional per-case semantic checks.

    The mechanics that must not fork per family live here: baseline/shared clash rejection,
    run-level and unknown-key rejection, the duplicate-id guard (the id is the
    pytest-benchmark compare-by-name boundary), and the mandatory `axis:` per variant --
    stamped on every case as `sweep_axis` ("baseline" for the implicit baseline case) so the
    swept axis is a queryable column, not something reverse-engineered from the id.
    """

    FRAMEWORK = None  # subclass pins
    PARAMS_CLASS = None  # subclass pins
    RUN_LEVEL_KEYS = ()

    def shared_keys(self, scenario, common_config):
        """Family-shared keys merged into every case (may read the run env)."""
        raise NotImplementedError

    def validate_case(self, params):
        """Optional family hook: raise ValueError for semantically invalid cases."""

    def build_cases(self, scenario, common_config):
        base = dict(common_config["baseline"])
        shared = dict(self.shared_keys(scenario, common_config))
        # A `baseline:` key colliding with a shared (common:/run-level) key would be
        # silently discarded by the {**base, **shared, **over} merge -- the yaml would say
        # one thing and the case run another. Reject instead of merging through it.
        clash = sorted(set(base) & set(shared))
        if clash:
            raise ValueError(
                f"configs.yaml baseline: sets key(s) {clash} that common: (or the run env) "
                "also sets; set these under common:, not baseline:"
            )
        cases = [self._make(base, {}, shared, sweep_axis="baseline")]
        for variant in scenario.get("variants", []):
            if "axis" not in variant:
                raise ValueError(
                    f"variant {variant!r} has no axis: name; every variant must say which "
                    "axis it perturbs (published as the sweep_axis column)"
                )
            over = {k: v for k, v in variant.items() if k != "axis"}
            cases.append(self._make(base, over, shared, sweep_axis=variant["axis"]))

        by_name = {}
        for c in cases:
            if c.name in by_name:
                raise ValueError(
                    f"two cases share the benchmark id {c.name!r}; either the variant "
                    "changes nothing, or it sweeps an axis benchmark_name() does not encode"
                )
            by_name[c.name] = c
        return list(by_name.values())

    def _make(self, base, over, shared, sweep_axis):
        import dataclasses

        clash = sorted(set(over) & set(self.RUN_LEVEL_KEYS))
        if clash:
            raise ValueError(
                f"variant overrides run-level key(s) {clash}; those come from the run env, "
                "not from configs.yaml"
            )
        cfg = {
            **base,
            **shared,
            **over,
            "name": "",
            "bucket_name": "",
            "framework": self.FRAMEWORK,
            "sweep_axis": sweep_axis,
        }
        valid = {f.name for f in dataclasses.fields(self.PARAMS_CLASS)}
        unknown = sorted(set(cfg) - valid)
        if unknown:
            raise ValueError(
                f"configs.yaml sets unknown key(s) {unknown} for {self.PARAMS_CLASS.__name__}"
            )
        p = self.PARAMS_CLASS(**cfg)
        self.validate_case(p)
        p.name = p.benchmark_name()
        return p
