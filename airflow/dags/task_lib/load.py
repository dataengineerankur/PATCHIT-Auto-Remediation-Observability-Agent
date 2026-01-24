from __future__ import annotations

import os
from dataclasses import dataclass

import duckdb


@dataclass(frozen=True)
class LoadResult:
    duckdb_path: str
    table: str


def load_csv_to_duckdb(*, csv_path: str, duckdb_path: str, scenario: str) -> LoadResult:
    os.makedirs(os.path.dirname(duckdb_path), exist_ok=True)
    con = duckdb.connect(duckdb_path)
    try:
        con.execute("create schema if not exists raw")
        con.execute("drop table if exists raw.api_payload")

        # Failure injection: create table without `amount` column (dbt_missing_column)
        if scenario == "dbt_missing_column":
            con.execute(
                """
                create table raw.api_payload as
                select run_id, event_time, user_id
                from read_csv_auto(?, header=true)
                """,
                [csv_path],
            )
        else:
            con.execute(
                """
                create table raw.api_payload as
                select run_id, event_time, user_id, amount
                from read_csv_auto(?, header=true)
                """,
                [csv_path],
            )
    finally:
        con.close()
    return LoadResult(duckdb_path=duckdb_path, table="raw.api_payload")



