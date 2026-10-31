from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import Column

BOT_DETECTOR_VERSION = "v1.0"

# Case-insensitive bot patterns:
# 1. Suffixes like [bot], -bot, _bot
# 2. Known automated automation accounts containing dependabot, github-actions, renovate, greenkeeper, snyk, codecov
BOT_PATTERNS = [
    r"\[bot\]$",
    r"-bot$",
    r"_bot$",
    r"\bdependabot\b",
    r"\bgithub-actions\b",
    r"\brenovate\b",
    r"\bgreenkeeper\b",
    r"\bsnyk\b",
    r"\bcodecov\b",
]

_COMPILED_PATTERN = re.compile("|".join(BOT_PATTERNS), re.IGNORECASE)
BOT_REGEX_SPARK = r"(\[bot\]|-bot|_bot)$|\b(dependabot|github-actions|renovate|greenkeeper|snyk|codecov)\b"


def is_bot_account(actor_login: str | None) -> bool:
    """
    Versioned, case-insensitive heuristic to classify whether an actor login is a bot.
    Returns False if actor_login is None or empty.
    """
    if not actor_login or not isinstance(actor_login, str):
        return False
    clean_login = actor_login.strip()
    return bool(_COMPILED_PATTERN.search(clean_login))


def is_bot_col(login_col: Column) -> Column:
    """
    Returns a PySpark boolean Column expression implementing the bot detection heuristic.
    """
    from pyspark.sql import functions as F

    return (
        F.when(login_col.isNull(), False)
        .when(F.lower(F.trim(login_col)).rlike(BOT_REGEX_SPARK), True)
        .otherwise(False)
    )
