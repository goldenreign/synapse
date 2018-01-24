# Copyright 2015, 2016 OpenMarket Ltd
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from synapse.storage.prepare_database import get_statements
from synapse.storage.engines import PostgresEngine, Sqlite3Engine

import ujson

logger = logging.getLogger(__name__)


POSTGRES_TABLE = """
CREATE EXTENSION pg_bigm;
CREATE EXTENSION unaccent;
CREATE OR REPLACE FUNCTION f_unaccent(text) RETURNS text AS $func$ SELECT public.unaccent('public.unaccent', $1) $func$ LANGUAGE sql IMMUTABLE;
ALTER TABLE event_search ADD COLUMN value text;
CREATE INDEX index_value_on_event_search_bigm ON event_search USING gin (f_unaccent(value) gin_bigm_ops);
"""


def run_create(cur, database_engine, *args, **kwargs):
    if isinstance(database_engine, PostgresEngine):
        for statement in get_statements(POSTGRES_TABLE.splitlines()):

            cur.execute(statement)

        cur.execute("SELECT MIN(stream_ordering) FROM events")
        rows = cur.fetchall()
        min_stream_id = rows[0][0]

        cur.execute("SELECT MAX(stream_ordering) FROM events")
        rows = cur.fetchall()
        max_stream_id = rows[0][0]

        if min_stream_id is not None and max_stream_id is not None:
            progress = {
                "target_min_stream_id_inclusive": min_stream_id,
                "max_stream_id_exclusive": max_stream_id + 1,
                "rows_inserted": 0,
            }
            progress_json = ujson.dumps(progress)

            sql = (
                "INSERT into background_updates (update_name, progress_json)"
                " VALUES (?, ?)"
            )

            sql = database_engine.convert_param_style(sql)

            cur.execute(sql, ("event_search_postgres_bigram", progress_json))
    elif isinstance(database_engine, Sqlite3Engine):
        pass
    else:
        raise Exception("Unrecognized database engine")


def run_upgrade(*args, **kwargs):
    pass
