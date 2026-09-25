"""Regression tests for inventory-exception matching only.

Does not import ParkWhiz, Gmail, or the worker loop.
"""
from __future__ import annotations

import datetime as dt
import unittest
from zoneinfo import ZoneInfo

from files.spothero_tool import (
    EventInfo,
    InventoryRule,
    _any_exception_ie_covers_event_start,
    _containing_controller,
)

TZ = ZoneInfo("America/Chicago")


def _dt(y: int, m: int, d: int, hh: int, mm: int = 0) -> dt.datetime:
    return dt.datetime(y, m, d, hh, mm, tzinfo=TZ)


def _rule(start: dt.datetime, end: dt.datetime, qty: int) -> InventoryRule:
    return InventoryRule(
        facility_id=1,
        valid_from_local=start,
        valid_to_local=end,
        quantity=qty,
        raw={"is_exception": True},
    )


def _event(start: dt.datetime, end: dt.datetime, inventory: int = 16) -> EventInfo:
    return EventInfo(
        event_id=1,
        rule_id=None,
        event_starts_local=start,
        event_ends_local=end,
        starts_offset="-05:00",
        ends_offset="-05:00",
        tiers=[{"price": 10, "order": 1, "inventory": inventory}],
        raw={},
    )


def _qty(rules: list[InventoryRule], ev: EventInfo):
    return _containing_controller(rules, ev)[0]


class KenmoreOvernightTests(unittest.TestCase):
    """4909 N Kenmore: Oct 9 4:30 PM → Oct 10 1:30 AM @ 15."""

    def setUp(self) -> None:
        self.ie = _rule(_dt(2026, 10, 9, 16, 30), _dt(2026, 10, 10, 1, 30), 15)
        self.rules = [self.ie]

    def test_oct_9_evening_uses_overnight_ie(self) -> None:
        ev = _event(_dt(2026, 10, 9, 19, 30), _dt(2026, 10, 9, 22, 30))
        self.assertEqual(_qty(self.rules, ev), 15)

    def test_oct_10_evening_does_not_use_1_30am_ie(self) -> None:
        ev = _event(_dt(2026, 10, 10, 19, 30), _dt(2026, 10, 10, 22, 30))
        self.assertIsNone(_qty(self.rules, ev))
        self.assertFalse(_any_exception_ie_covers_event_start(self.rules, ev))

    def test_oct_10_early_morning_still_overlaps(self) -> None:
        ev = _event(_dt(2026, 10, 10, 0, 30), _dt(2026, 10, 10, 1, 0))
        self.assertEqual(_qty(self.rules, ev), 15)


class HighlandOvernightTests(unittest.TestCase):
    """Highland: overnight @ 2 vs cancelled Sep 18–30 @ 3."""

    def setUp(self) -> None:
        self.overnight = _rule(_dt(2026, 9, 27, 18, 30), _dt(2026, 9, 28, 0, 30), 2)
        self.cancelled = _rule(_dt(2026, 9, 18, 0, 0), _dt(2026, 9, 30, 0, 0), 3)
        self.rules = [self.overnight, self.cancelled]

    def test_sep_27_afternoon_uses_narrow_overnight(self) -> None:
        ev = _event(_dt(2026, 9, 27, 14, 0), _dt(2026, 9, 27, 17, 0), inventory=2)
        self.assertEqual(_qty(self.rules, ev), 2)

    def test_sep_28_evening_does_not_take_cancelled_or_overnight(self) -> None:
        ev = _event(_dt(2026, 9, 28, 18, 0), _dt(2026, 9, 28, 23, 0), inventory=2)
        self.assertIsNone(_qty(self.rules, ev))
        self.assertFalse(_any_exception_ie_covers_event_start(self.rules, ev))


class EgmontMidnightTests(unittest.TestCase):
    """76 Egmont: Sep 15 4:30 PM → Sep 16 12:00 AM @ 3. Baseline is 2."""

    def setUp(self) -> None:
        self.ie = _rule(_dt(2026, 9, 15, 16, 30), _dt(2026, 9, 16, 0, 0), 3)
        self.rules = [self.ie]

    def test_sep_15_evening_uses_ie(self) -> None:
        ev = _event(_dt(2026, 9, 15, 20, 0), _dt(2026, 9, 16, 0, 0), inventory=3)
        self.assertEqual(_qty(self.rules, ev), 3)

    def test_sep_16_evening_stays_at_baseline(self) -> None:
        ev = _event(_dt(2026, 9, 16, 20, 0), _dt(2026, 9, 17, 0, 0), inventory=2)
        self.assertIsNone(_qty(self.rules, ev))
        self.assertFalse(_any_exception_ie_covers_event_start(self.rules, ev))


class DeanStMidnightTests(unittest.TestCase):
    """Multi-day midnight Valid To still covers that entire last calendar day."""

    def test_oct_8_evening_uses_midnight_ie_through_oct_9(self) -> None:
        ie = _rule(_dt(2026, 10, 6, 0, 0), _dt(2026, 10, 9, 0, 0), 0)
        ev = _event(_dt(2026, 10, 8, 18, 0), _dt(2026, 10, 8, 22, 0), inventory=10)
        self.assertEqual(_qty([ie], ev), 0)

    def test_oct_8_evening_uses_midnight_ie_ending_oct_8(self) -> None:
        ie = _rule(_dt(2026, 10, 6, 0, 0), _dt(2026, 10, 8, 0, 0), 0)
        ev = _event(_dt(2026, 10, 8, 18, 0), _dt(2026, 10, 8, 22, 0), inventory=10)
        self.assertEqual(_qty([ie], ev), 0)


class BrowneSameDayTests(unittest.TestCase):
    """Same-day 3–5 PM IE must not control 7–10 PM."""

    def test_evening_event_ignores_afternoon_ie(self) -> None:
        ie = _rule(_dt(2026, 9, 20, 15, 0), _dt(2026, 9, 20, 17, 0), 0)
        ev = _event(_dt(2026, 9, 20, 19, 0), _dt(2026, 9, 20, 22, 0), inventory=2)
        self.assertIsNone(_qty([ie], ev))
        self.assertFalse(_any_exception_ie_covers_event_start([ie], ev))

    def test_afternoon_event_uses_afternoon_ie(self) -> None:
        ie = _rule(_dt(2026, 9, 20, 15, 0), _dt(2026, 9, 20, 17, 0), 0)
        ev = _event(_dt(2026, 9, 20, 15, 0), _dt(2026, 9, 20, 17, 0), inventory=2)
        self.assertEqual(_qty([ie], ev), 0)


class HighlandStartInWindowTests(unittest.TestCase):
    """3737 Highland: 1-stall to Sep 16 7 PM vs cancelled 2-stall Sep 11–18."""

    def setUp(self) -> None:
        self.one = _rule(_dt(2026, 9, 15, 12, 0), _dt(2026, 9, 16, 19, 0), 1)
        self.two = _rule(_dt(2026, 9, 11, 23, 0), _dt(2026, 9, 18, 23, 0), 2)
        self.rules = [self.one, self.two]

    def test_sep_16_5pm_uses_1_stall(self) -> None:
        ev = _event(_dt(2026, 9, 16, 17, 0), _dt(2026, 9, 16, 22, 0), inventory=1)
        self.assertEqual(_qty(self.rules, ev), 1)

    def test_sep_16_730pm_after_1_stall_uses_cancelled_2(self) -> None:
        ev = _event(_dt(2026, 9, 16, 19, 30), _dt(2026, 9, 16, 22, 30), inventory=1)
        self.assertEqual(_qty(self.rules, ev), 2)

    def test_sep_17_uses_cancelled_2(self) -> None:
        ev = _event(_dt(2026, 9, 17, 17, 0), _dt(2026, 9, 17, 22, 0), inventory=2)
        self.assertEqual(_qty(self.rules, ev), 2)


class BurlingNextIeTests(unittest.TestCase):
    """2828 N Burling: 0-stall ends midnight, 1-stall starts midnight."""

    def setUp(self) -> None:
        self.zero = _rule(_dt(2026, 9, 15, 17, 0), _dt(2026, 9, 16, 0, 0), 0)
        self.one = _rule(_dt(2026, 9, 16, 0, 0), _dt(2026, 9, 17, 7, 0), 1)
        self.cancelled = _rule(_dt(2026, 9, 2, 0, 0), _dt(2026, 9, 16, 0, 0), 1)
        self.rules = [self.zero, self.one, self.cancelled]

    def test_sep_16_evening_uses_1_stall_not_baseline(self) -> None:
        ev = _event(_dt(2026, 9, 16, 18, 40), _dt(2026, 9, 16, 21, 40), inventory=1)
        self.assertEqual(_qty(self.rules, ev), 1)
        self.assertTrue(_any_exception_ie_covers_event_start(self.rules, ev))

    def test_sep_16_evening_is_not_zero(self) -> None:
        ev = _event(_dt(2026, 9, 16, 18, 40), _dt(2026, 9, 16, 21, 40), inventory=1)
        self.assertNotEqual(_qty(self.rules, ev), 0)

    def test_sep_15_evening_uses_zero(self) -> None:
        ev = _event(_dt(2026, 9, 15, 18, 40), _dt(2026, 9, 15, 21, 40), inventory=2)
        self.assertEqual(_qty(self.rules, ev), 0)


class StoneholmSameDayHandoffTests(unittest.TestCase):
    """12 Stoneholm: 5–8 PM @ 24 then 8–11 PM @ 25."""

    def setUp(self) -> None:
        self.r24 = _rule(_dt(2026, 9, 22, 17, 0), _dt(2026, 9, 22, 20, 0), 24)
        self.r25 = _rule(_dt(2026, 9, 22, 20, 0), _dt(2026, 9, 22, 23, 0), 25)
        self.rules = [self.r24, self.r25]

    def test_630pm_stays_24(self) -> None:
        ev = _event(_dt(2026, 9, 22, 18, 30), _dt(2026, 9, 22, 21, 30), inventory=24)
        self.assertEqual(_qty(self.rules, ev), 24)

    def test_645pm_stays_24(self) -> None:
        ev = _event(_dt(2026, 9, 22, 18, 45), _dt(2026, 9, 22, 21, 45), inventory=24)
        self.assertEqual(_qty(self.rules, ev), 24)

    def test_800pm_uses_25(self) -> None:
        ev = _event(_dt(2026, 9, 22, 20, 0), _dt(2026, 9, 22, 23, 0), inventory=25)
        self.assertEqual(_qty(self.rules, ev), 25)


class LeastStallsStartInWindowTests(unittest.TestCase):
    """5-stall and shorter cancelled 6-stall both cover Sep 25–26."""

    def setUp(self) -> None:
        self.five = _rule(_dt(2026, 9, 22, 12, 30), _dt(2026, 9, 27, 23, 0), 5)
        self.canc6 = _rule(_dt(2026, 9, 23, 0, 0), _dt(2026, 9, 27, 23, 0), 6)
        self.cont6 = _rule(_dt(2026, 9, 27, 23, 0), _dt(2026, 9, 29, 20, 0), 6)
        self.rules = [self.five, self.canc6, self.cont6]

    def test_sep_25_uses_5_not_cancelled_6(self) -> None:
        ev = _event(_dt(2026, 9, 25, 19, 0), _dt(2026, 9, 25, 22, 0), inventory=6)
        self.assertEqual(_qty(self.rules, ev), 5)

    def test_sep_26_uses_5_not_cancelled_6(self) -> None:
        ev = _event(_dt(2026, 9, 26, 19, 0), _dt(2026, 9, 26, 22, 0), inventory=6)
        self.assertEqual(_qty(self.rules, ev), 5)

    def test_sep_28_uses_cont_6_after_five_ends(self) -> None:
        ev = _event(_dt(2026, 9, 28, 19, 0), _dt(2026, 9, 28, 22, 0), inventory=6)
        self.assertEqual(_qty(self.rules, ev), 6)


class WestlandMultidayTests(unittest.TestCase):
    """Multi-day 8 PM–6 AM still controls the middle evening."""

    def test_middle_evening_uses_ie(self) -> None:
        ie = _rule(_dt(2026, 9, 16, 20, 0), _dt(2026, 9, 18, 6, 0), 0)
        ev = _event(_dt(2026, 9, 17, 20, 0), _dt(2026, 9, 17, 23, 0), inventory=5)
        self.assertEqual(_qty([ie], ev), 0)

    def test_end_date_daytime_after_6am_is_not_controlled(self) -> None:
        ie = _rule(_dt(2026, 9, 16, 20, 0), _dt(2026, 9, 18, 6, 0), 0)
        ev = _event(_dt(2026, 9, 18, 10, 0), _dt(2026, 9, 18, 14, 0), inventory=5)
        self.assertIsNone(_qty([ie], ev))


if __name__ == "__main__":
    unittest.main()
