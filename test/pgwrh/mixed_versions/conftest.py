"""Require both installations when this opt-in suite is selected."""
import pytest

from ..pgwrh_testkit import PostgresInstallation


@pytest.fixture(scope="session")
def postgres_installations():
    return {major: PostgresInstallation.from_env(major) for major in (18, 19)}
