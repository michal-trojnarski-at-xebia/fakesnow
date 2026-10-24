# pyright: reportOptionalMemberAccess=false

import shutil
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
import snowflake.connector

import fakesnow

dbt_main = pytest.importorskip("dbt.cli.main")

if TYPE_CHECKING:
    from dbt.cli.main import dbtRunnerResult

PROJECT = Path(__file__).parent / "dbt_project"


def copy_project(tmp_path: Path) -> Path:
    # dbt writes target/ and logs/ into the project dir, so run from a copy
    project = tmp_path / "project"
    shutil.copytree(PROJECT, project)
    return project


def append_seed_row(project: Path, row: str) -> None:
    with (project / "seeds" / "raw_orders.csv").open("a") as f:
        f.write(f"{row}\n")


def run_dbt(project_dir: Path, *args: str) -> "dbtRunnerResult":
    result = dbt_main.dbtRunner().invoke([*args, "--project-dir", str(project_dir), "--profiles-dir", str(project_dir)])
    assert result.success, f"dbt {' '.join(args)} failed: {result.exception!r}, see {project_dir}/logs/dbt.log"
    return result


def test_dbt_build_twice_patch_mode(tmp_path: Path) -> None:
    project = copy_project(tmp_path)
    with fakesnow.patch():
        for _ in range(2):
            run_dbt(project, "seed")
            run_dbt(project, "run")
            run_dbt(project, "snapshot")
            run_dbt(project, "test")
        with snowflake.connector.connect(database="dbt_db", schema="analytics") as conn:
            cur = conn.cursor()
            assert cur.execute("select count(*) from inc_merge").fetchall() == [(3,)]
            assert cur.execute("select count(*) from inc_delete_insert").fetchall() == [(3,)]
            # append re-inserts rows newer than the max, of which there are none on the second run
            assert cur.execute("select count(*) from inc_append").fetchall() == [(3,)]
            assert cur.execute("select count(*) from snapshots.orders_snap").fetchall() == [(3,)]


def test_dbt_incremental_picks_up_new_rows_patch_mode(tmp_path: Path) -> None:
    project = copy_project(tmp_path)
    with fakesnow.patch():
        run_dbt(project, "seed")
        run_dbt(project, "run")
        append_seed_row(project, "4,dave,40,2024-01-04 00:00:00")
        run_dbt(project, "seed")
        run_dbt(project, "run")
        with snowflake.connector.connect(database="dbt_db", schema="analytics") as conn:
            assert conn.cursor().execute("select count(*) from inc_merge").fetchall() == [(4,)]


def test_dbt_build_twice_server_mode(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project = copy_project(tmp_path)
    db_path = tmp_path / "db"
    db_path.mkdir()
    with fakesnow.server(db_path=str(db_path)) as kwargs:
        monkeypatch.setenv("FAKESNOW_HOST", kwargs["host"])
        monkeypatch.setenv("FAKESNOW_PORT", str(kwargs["port"]))
        monkeypatch.setenv("FAKESNOW_PROTOCOL", "http")
        for _ in range(2):
            run_dbt(project, "build")
        run_dbt(project, "docs", "generate")
