"""
SQLite backend for the sqlite3 module in the standard library.
"""
import asyncio
import decimal
import functools
import random
import statistics
from itertools import chain

import aiosqlite as Database

from django.core.exceptions import ImproperlyConfigured
from django.db import IntegrityError
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.sqlite3.base import (
    BaseSQLiteDatabaseWrapper, decoder, list_aggregate, FORMAT_QMARK_REGEX
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


class DatabaseWrapper(BaseSQLiteDatabaseWrapper):
    Database = Database
    SchemaEditorClass = DatabaseSchemaEditor
    # Classes instantiated in __init__().
    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations

    async def aget_new_connection(self, conn_params):
        conn = Database.connect(**conn_params)
        create_deterministic_function = functools.partial(
            conn.create_function,
            deterministic=True,
        )
        await asyncio.gather(self.create_conn_functions(create_deterministic_function))
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

    async def aclose(self):
        self.validate_thread_sharing()
        if not self.is_in_memory_db():
            await BaseDatabaseWrapper.aclose(self)

    async def _asavepoint_allowed(self):
        return self.in_atomic_block

    async def _aset_autocommit(self, autocommit):
        if autocommit:
            level = None
        else:
            # sqlite3's internal default is ''. It's different from None.
            # See Modules/_sqlite/connection.c.
            level = ''
        # 'isolation_level' is a misleading API.
        # SQLite always runs at the SERIALIZABLE isolation level.
        with self.wrap_database_errors:
            self.connection.isolation_level = level

    async def adisable_constraint_checking(self):
        async with self.acursor() as cursor:
            await cursor.execute('PRAGMA foreign_keys = OFF')
            # Foreign key constraints cannot be turned off while in a multi-
            # statement transaction. Fetch the current state of the pragma
            # to determine if constraints are effectively disabled.
            enabled = await cursor.execute('PRAGMA foreign_keys').fetchone()[0]
        return not bool(enabled)

    async def aenable_constraint_checking(self):
        async with self.acursor() as cursor:
            await cursor.execute('PRAGMA foreign_keys = ON')

    async def acheck_constraints(self, table_names=None):
        """
        Check each table name in `table_names` for rows with invalid foreign
        key references. This method is intended to be used in conjunction with
        `disable_constraint_checking()` and `enable_constraint_checking()`, to
        determine if rows with invalid references were entered while constraint
        checks were off.
        """
        if self.features.supports_pragma_foreign_key_check:
            async with self.acursor() as cursor:
                if table_names is None:
                    violations = await cursor.execute_fetchall('PRAGMA foreign_key_check')
                else:
                    violations = chain.from_iterable(
                        await asyncio.gather(
                            *(
                                cursor.execute_fetchall(
                                    'PRAGMA foreign_key_check(%s)'
                                    % self.ops.quote_name(table_name)
                                )
                                for table_name in table_names
                            )
                        )
                    )
                # See https://www.sqlite.org/pragma.html#pragma_foreign_key_check
                for table_name, rowid, referenced_table_name, foreign_key_index in violations:
                    foreign_key = (
                        await cursor.execute_fetchall(
                            'PRAGMA foreign_key_list(%s)' % self.ops.quote_name(table_name)
                        )
                    )[foreign_key_index]
                    column_name, referenced_column_name = foreign_key[3:5]
                    primary_key_column_name = await self.introspection.aget_primary_key_column(cursor, table_name)
                    primary_key_value, bad_value = await cursor.execute_fetchone(
                        'SELECT %s, %s FROM %s WHERE rowid = %%s' % (
                            self.ops.quote_name(primary_key_column_name),
                            self.ops.quote_name(column_name),
                            self.ops.quote_name(table_name),
                        ),
                        (rowid,),
                    )
                    raise IntegrityError(
                        "The row in table '%s' with primary key '%s' has an "
                        "invalid foreign key: %s.%s contains a value '%s' that "
                        "does not have a corresponding value in %s.%s." % (
                            table_name, primary_key_value, table_name, column_name,
                            bad_value, referenced_table_name, referenced_column_name
                        )
                    )
        else:
            async with self.acursor() as cursor:
                if table_names is None:
                    table_names = await self.introspection.atable_names(cursor)
                for table_name in table_names:
                    primary_key_column_name = await self.introspection.aget_primary_key_column(cursor, table_name)
                    if not primary_key_column_name:
                        continue
                    relations = await self.introspection.aget_relations(cursor, table_name)
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

    def is_usable(self):
        return True

    async def _astart_transaction_under_autocommit(self):
        """
        Start a transaction explicitly in autocommit mode.

        Staying in autocommit mode works around a bug of sqlite3 that breaks
        savepoints when autocommit is disabled.
        """
        await (await self.acursor()).execute("BEGIN")

    def is_in_memory_db(self):
        return self.creation.is_in_memory_db(self.settings_dict['NAME'])


class SQLiteCursorWrapper(Database.Cursor):
    """
    Django uses "format" style placeholders, but pysqlite2 uses "qmark" style.
    This fixes it -- but note that if you want to use a literal "%s" in a query,
    you'll need to use "%%s".
    """

    async def aexecute(self, query, params=None):
        if params is None:
            return await Database.Cursor.execute(self, query)
        query = self.convert_query(query)
        return await Database.Cursor.execute(self, query, params)

    async def aexecutemany(self, query, param_list):
        query = self.convert_query(query)
        return await Database.Cursor.executemany(self, query, param_list)

    def convert_query(self, query):
        return FORMAT_QMARK_REGEX.sub('?', query).replace('%%', '%')
