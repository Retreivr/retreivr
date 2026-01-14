import unittest

from engine import core as engine_core


class YtdlpAttemptPlanTests(unittest.TestCase):
    def test_attempt_plan_has_default_and_best(self):
        plan = engine_core._build_download_attempt_plan("strict")
        has_default = any(step.get("extractor_args") is None for step in plan)
        has_best = any("best" in (step.get("format") or "") for step in plan)
        self.assertTrue(has_default)
        self.assertTrue(has_best)
