"""Parse Snowflake SQL with Snowflake-compatible syntax errors."""

import re

import snowflake.connector.errors
import sqlglot
from sqlglot import Expr, exp
from sqlglot.errors import ParseError

_DESCRIBE_TASK = re.compile(r"\s*(?:DESC|DESCRIBE)\s+(?=TASK\b)", re.IGNORECASE)


def parse(command: str) -> list[Expr | None]:
    try:
        return sqlglot.parse(command, read="snowflake")
    except ParseError as error:
        raise syntax_error(error) from None


def parse_one(command: str) -> Expr:
    try:
        return sqlglot.parse_one(command, read="snowflake")
    except ParseError as error:
        # sqlglot can't parse DESCRIBE TASK, so hand it on as a command like the other task statements
        if match := _DESCRIBE_TASK.match(command):
            return exp.Command(this="DESCRIBE", expression=exp.Literal.string(command[match.end() :].strip()))
        raise syntax_error(error) from None


def syntax_error(error: ParseError) -> snowflake.connector.errors.ProgrammingError:
    # Strip terminal highlighting from SQLGlot's error message.
    message = str(error).replace("\x1b[4m", "").replace("\x1b[0m", "")
    return snowflake.connector.errors.ProgrammingError(msg=message, errno=1003, sqlstate="42000")
