"""
SQLite backend for the sqlite3 module in the standard library.
"""
import decimal
import functools
import random
import statistics
from itertools import chain

import aiosqlite as Database

from django.core.exceptions import ImproperlyConfigured
from django.db import IntegrityError
from django.db.backends.base.base import BaseAsyncDatabaseWrapper
from django.db.backends.sqlite3.base import (
    FORMAT_QMARK_REGEX, DatabaseWrapper as SQLiteDatabaseWrapper, decoder,
    list_aggregate,
)
from django.utils.dateparse import parse_datetime, parse_time

from .client import DatabaseClient
from .creation import DatabaseCreation
from .features import DatabaseFeatures
from .introspection import DatabaseIntrospection
from .operations import DatabaseOperations
from .schema import DatabaseSchemaEditor


def check_sqlite_version():
    if Database.sqlite_version_info < (3, 9, 0):
        raise ImproperlyConfigured(
            'SQLite 3.9.0 or later is required (found %s).' % Database.sqlite_version
        )


check_sqlite_version()

Database.register_converter("bool", b'1'.__eq__)
Database.register_converter("time", decoder(parse_time))
Database.register_converter("datetime", decoder(parse_datetime))
Database.register_converter("timestamp", decoder(parse_datetime))

Database.register_adapter(decimal.Decimal, str)


class DatabaseWrapper(SQLiteDatabaseWrapper, BaseAsyncDatabaseWrapper):
    Database = Database
    SchemaEditorClass = DatabaseSchemaEditor
    # Classes instantiated in __init__().
    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations

    async def get_new_connection(self, conn_params):
        conn = Database.connect(**conn_params)
        create_deterministic_function = functools.partial(
            conn.create_function,
            deterministic=True,
        )
        for func in self.create_conn_functions(create_deterministic_function):
            await func()
        # Don't use the built-in RANDOM() function because it returns a value
        # in the range [-1 * 2^63, 2^63 - 1] instead of [0, 1).
        await conn.create_function('RAND', 0, random.random)

        async def create_aggregate(*args):
            await conn._execute(conn._conn.create_aggregate, *args)

        await create_aggregate('STDDEV_POP', 1, list_aggregate(statistics.pstdev))
        await create_aggregate('STDDEV_SAMP', 1, list_aggregate(statistics.stdev))
        await create_aggregate('VAR_POP', 1, list_aggregate(statistics.pvariance))
        await create_aggregate('VAR_SAMP', 1, list_aggregate(statistics.variance))
        await conn.execute('PRAGMA foreign_keys = ON')
        return conn

    def create_cursor(self, name=None):
        return self.connection.cursor(factory=SQLiteCursorWrapper)

    async def close(self):
        await self.validate_task_sharing()
        # If database is in memory, closing the connection destroys the
        # database. To prevent accidental data loss, ignore close requests on
        # an in-memory db.
        if not self.is_in_memory_db():
            await BaseAsyncDatabaseWrapper.close(self)

    async def _savepoint_allowed(self):
        # When 'isolation_level' is not None, sqlite3 commits before each
        # savepoint; it's a bug. When it is None, savepoints don't make sense
        # because autocommit is enabled. The only exception is inside 'atomic'
        # blocks. To work around that bug, on SQLite, 'atomic' starts a
        # transaction explicitly rather than simply disable autocommit.
        return self.in_atomic_block

    async def disable_constraint_checking(self):
        with await self.cursor() as cursor:
            await cursor.execute('PRAGMA foreign_keys = OFF')
            # Foreign key constraints cannot be turned off while in a multi-
            # statement transaction. Fetch the current state of the pragma
            # to determine if constraints are effectively disabled.
            await cursor.execute('PRAGMA foreign_keys')
            enabled = (await cursor.fetchone())[0]
        return not bool(enabled)

    async def enable_constraint_checking(self):
        with await self.cursor() as cursor:
            await cursor.execute('PRAGMA foreign_keys = ON')

    async def check_constraints(self, table_names=None):
        """See SQLiteDatabaseWrapper.check_constraints()."""
        if self.features.supports_pragma_foreign_key_check:
            with await self.cursor() as cursor:
                if table_names is None:
                    violations = await cursor.execute('PRAGMA foreign_key_check').fetchall()
                else:
                    violations = chain.from_iterable(
                        await (
                            await cursor.execute(
                                'PRAGMA foreign_key_check(%s)'
                                % self.ops.quote_name(table_name)
                            )
                        ).fetchall()
                        for table_name in table_names
                    )
                # See https://www.sqlite.org/pragma.html#pragma_foreign_key_check
                for table_name, rowid, referenced_table_name, foreign_key_index in violations:
                    await cursor.execute('PRAGMA foreign_key_list(%s)' % self.ops.quote_name(table_name))
                    foreign_key = (await cursor.fetchall())[foreign_key_index]
                    column_name, referenced_column_name = foreign_key[3:5]
                    primary_key_column_name = await self.introspection.get_primary_key_column(cursor, table_name)
                    await cursor.execute(
                        'SELECT %s, %s FROM %s WHERE rowid = %%s' % (
                            self.ops.quote_name(primary_key_column_name),
                            self.ops.quote_name(column_name),
                            self.ops.quote_name(table_name),
                        ),
                        (rowid,),
                    )
                    primary_key_value, bad_value = await cursor.fetchone()
                    raise IntegrityError(
                        "The row in table '%s' with primary key '%s' has an "
                        "invalid foreign key: %s.%s contains a value '%s' that "
                        "does not have a corresponding value in %s.%s." % (
                            table_name, primary_key_value, table_name, column_name,
                            bad_value, referenced_table_name, referenced_column_name
                        )
                    )
        else:
            with await self.cursor() as cursor:
                if table_names is None:
                    table_names = await self.introspection.table_names(cursor)
                for table_name in table_names:
                    primary_key_column_name = await self.introspection.get_primary_key_column(cursor, table_name)
                    if not primary_key_column_name:
                        continue
                    relations = await self.introspection.get_relations(cursor, table_name)
                    for column_name, (referenced_column_name, referenced_table_name) in relations:
                        await cursor.execute(
                            """
                            SELECT REFERRING.`%s`, REFERRING.`%s` FROM `%s` as REFERRING
                            LEFT JOIN `%s` as REFERRED
                            ON (REFERRING.`%s` = REFERRED.`%s`)
                            WHERE REFERRING.`%s` IS NOT NULL AND REFERRED.`%s` IS NULL
                            """
                            % (
                                primary_key_column_name, column_name, table_name,
                                referenced_table_name, column_name, referenced_column_name,
                                column_name, referenced_column_name,
                            )
                        )
                        for bad_row in await cursor.fetchall():
                            raise IntegrityError(
                                "The row in table '%s' with primary key '%s' has an "
                                "invalid foreign key: %s.%s contains a value '%s' that "
                                "does not have a corresponding value in %s.%s." % (
                                    table_name, bad_row[0], table_name, column_name,
                                    bad_row[1], referenced_table_name, referenced_column_name,
                                )
                            )

    async def is_usable(self):
        return True

    async def _start_transaction_under_autocommit(self):
        """
        Start a transaction explicitly in autocommit mode.

        Staying in autocommit mode works around a bug of sqlite3 that breaks
        savepoints when autocommit is disabled.
        """
        await (await self.cursor()).execute("BEGIN")


class SQLiteCursorWrapper(Database.Cursor):
    """
    See django.db.backends.sqlite.base.SQLiteCursorWrapper
    """

    async def execute(self, query, params=None):
        if params is None:
            return await Database.Cursor.execute(self, query)
        query = self.convert_query(query)
        return await Database.Cursor.execute(self, query, params)

    async def executemany(self, query, param_list):
        query = self.convert_query(query)
        return await Database.Cursor.executemany(self, query, param_list)

    def convert_query(self, query):
        return FORMAT_QMARK_REGEX
