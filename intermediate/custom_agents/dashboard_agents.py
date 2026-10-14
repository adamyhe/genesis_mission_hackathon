"""The academy_dashboard example agents, launched from this package.

Globus Compute sends a class by its module path, so a remote worker that runs
these agents imports intermediate.custom_agents first, and the exchange
reconnect fix is in place before they listen.
"""

from __future__ import annotations

from academy_dashboard.agents import Sleeper as _Sleeper
from academy_dashboard.agents import Spinner as _Spinner


class Spinner(_Spinner):
    """academy_dashboard Spinner with the exchange reconnect fix."""


class Sleeper(_Sleeper):
    """academy_dashboard Sleeper with the exchange reconnect fix."""
