#!/usr/bin/env python3
"""
Database connection and basic operations management
"""

import logging
from contextlib import contextmanager
from typing import Optional, Any, List, Tuple, Dict
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool, QueuePool

from config.settings import DatabaseConfig

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Manages database connections and basic operations"""

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.engine: Optional[Engine] = None

    def connect(self) -> bool:
        """Establish database connection"""
        try:
            # Create engine with connection pooling
            self.engine = create_engine(
                self.config.connection_string,
                pool_pre_ping=True,
                pool_recycle=300,
                pool_size=10,
                max_overflow=20,
                echo=False  # Set to True for SQL debugging
            )
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("Database connection established")
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False

    def disconnect(self):
        """Close database connection"""
        if self.engine:
            self.engine.dispose()
            self.engine = None
        logger.info("Database connection closed")

    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        if not self.engine:
            raise RuntimeError("Database not connected")

        conn = self.engine.connect()
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def begin_transaction(self):
        """Context manager for database transactions"""
        if not self.engine:
            raise RuntimeError("Database not connected")

        with self.engine.begin() as conn:
            yield conn

    def execute_query(self, query: str, params: Optional[dict] = None) -> Any:
        """Execute a query and return results"""
        if not self.engine:
            raise RuntimeError("Database not connected")

        with self.engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            return result

    def execute_transaction(self, query: str, params: Optional[dict] = None) -> Any:
        """Execute a query within a transaction"""
        if not self.engine:
            raise RuntimeError("Database not connected")

        with self.engine.begin() as conn:
            result = conn.execute(text(query), params or {})
            return result

    def execute_many(self, query: str, data: List[dict]) -> int:
        """Execute a query with multiple parameter sets"""
        if not self.engine:
            raise RuntimeError("Database not connected")

        if not data:
            return 0

        with self.engine.begin() as conn:
            # Convert query to use bindparams if needed
            stmt = text(query)

            # Execute in batches for better performance
            batch_size = 1000
            total_affected = 0

            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                result = conn.execute(stmt, batch)
                total_affected += result.rowcount

            return total_affected

    def execute_batch_insert(self, table: str, schema: str, data: List[dict]) -> int:
        """Execute batch insert using SQLAlchemy's executemany"""
        if not self.engine:
            raise RuntimeError("Database not connected")

        if not data:
            return 0

        # Build insert statement dynamically
        columns = list(data[0].keys())
        placeholders = ', '.join([f":{col}" for col in columns])
        column_names = ', '.join(columns)

        insert_sql = f"""
            INSERT INTO {schema}.{table} ({column_names})
            VALUES ({placeholders})
        """

        return self.execute_many(insert_sql, data)

    def fetch_one(self, query: str, params: Optional[dict] = None) -> Optional[Tuple]:
        """Execute query and fetch one result"""
        result = self.execute_query(query, params)
        return result.fetchone()

    def fetch_all(self, query: str, params: Optional[dict] = None) -> List[Tuple]:
        """Execute query and fetch all results"""
        result = self.execute_query(query, params)
        return result.fetchall()

    def fetch_dict(self, query: str, params: Optional[dict] = None) -> List[Dict[str, Any]]:
        """Execute query and return results as list of dictionaries"""
        result = self.execute_query(query, params)
        columns = result.keys()
        return [dict(zip(columns, row)) for row in result.fetchall()]

    def test_connection(self) -> bool:
        """Test if database connection is active"""
        try:
            if not self.engine:
                return False

            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    def check_extension(self, extension_name: str) -> bool:
        """Check if a PostgreSQL extension is installed"""
        try:
            result = self.execute_query(
                "SELECT 1 FROM pg_extension WHERE extname = :ext_name",
                {"ext_name": extension_name}
            )
            return result.rowcount > 0
        except Exception as e:
            logger.error(f"Error checking extension {extension_name}: {e}")
            return False

    def install_extension(self, extension_name: str) -> bool:
        """Install a PostgreSQL extension"""
        try:
            self.execute_transaction(f"CREATE EXTENSION IF NOT EXISTS {extension_name}")
            logger.info(f"Installed extension: {extension_name}")
            return True
        except Exception as e:
            logger.warning(f"Could not install extension {extension_name}: {e}")
            return False

    def table_exists(self, schema: str, table: str) -> bool:
        """Check if a table exists"""
        try:
            result = self.execute_query(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = :schema
                  AND table_name = :table
                """,
                {"schema": schema, "table": table}
            )
            return result.rowcount > 0
        except Exception as e:
            logger.error(f"Error checking table {schema}.{table}: {e}")
            return False

    def get_table_count(self, schema: str, table: str) -> int:
        """Get row count for a table"""
        try:
            result = self.execute_query(f"SELECT COUNT(*) FROM {schema}.{table}")
            row = result.fetchone()
            return row[0] if row else 0
        except Exception as e:
            logger.error(f"Error getting count for {schema}.{table}: {e}")
            return 0

    def get_table_size(self, schema: str, table: str) -> Dict[str, Any]:
        """Get table size information"""
        try:
            query = """
                    SELECT pg_size_pretty(pg_total_relation_size(schemaname || '.' || tablename)) as total_size, \
                           pg_size_pretty(pg_relation_size(schemaname || '.' || tablename))       as table_size, \
                           pg_size_pretty(pg_indexes_size(schemaname || '.' || tablename))        as indexes_size, \
                           pg_total_relation_size(schemaname || '.' || tablename)                 as total_bytes
                    FROM pg_tables
                    WHERE schemaname = :schema \
                      AND tablename = :table \
                    """

            result = self.fetch_one(query, {"schema": schema, "table": table})

            if result:
                return {
                    "total_size": result[0],
                    "table_size": result[1],
                    "indexes_size": result[2],
                    "total_bytes": result[3]
                }
            return {}

        except Exception as e:
            logger.error(f"Error getting size for {schema}.{table}: {e}")
            return {}

    def get_hypertables(self) -> List[Tuple[str, int]]:
        """Get list of TimescaleDB hypertables and their chunk counts"""
        try:
            result = self.execute_query(
                """
                SELECT hypertable_schema || '.' || hypertable_name as hypertable_name,
                       COALESCE(
                               (SELECT COUNT(*)
                                FROM timescaledb_information.chunks c
                                WHERE c.hypertable_schema = h.hypertable_schema
                                  AND c.hypertable_name = h.hypertable_name),
                               0
                       )                                           as num_chunks
                FROM timescaledb_information.hypertables h
                ORDER BY hypertable_name
                """
            )
            return [(row[0], row[1]) for row in result]
        except Exception as e:
            logger.error(f"Error getting hypertables: {e}")
            return []

    def get_table_columns(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """Get column information for a table"""
        try:
            query = """
                    SELECT column_name, \
                           data_type, \
                           is_nullable, \
                           column_default, \
                           character_maximum_length, \
                           numeric_precision, \
                           numeric_scale
                    FROM information_schema.columns
                    WHERE table_schema = :schema
                      AND table_name = :table
                    ORDER BY ordinal_position \
                    """

            result = self.execute_query(query, {"schema": schema, "table": table})

            columns = []
            for row in result:
                columns.append({
                    "name": row[0],
                    "type": row[1],
                    "nullable": row[2] == 'YES',
                    "default": row[3],
                    "max_length": row[4],
                    "precision": row[5],
                    "scale": row[6]
                })

            return columns

        except Exception as e:
            logger.error(f"Error getting columns for {schema}.{table}: {e}")
            return []

    def get_table_indexes(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """Get index information for a table"""
        try:
            query = """
                    SELECT indexname, \
                           indexdef, \
                           tablespace
                    FROM pg_indexes
                    WHERE schemaname = :schema
                      AND tablename = :table
                    ORDER BY indexname \
                    """

            result = self.execute_query(query, {"schema": schema, "table": table})

            indexes = []
            for row in result:
                indexes.append({
                    "name": row[0],
                    "definition": row[1],
                    "tablespace": row[2]
                })

            return indexes

        except Exception as e:
            logger.error(f"Error getting indexes for {schema}.{table}: {e}")
            return []

    def get_database_size(self) -> Dict[str, Any]:
        """Get database size information"""
        try:
            query = """
                SELECT 
                    pg_database_size(current_database()) as size_bytes,
                    pg_size_pretty(pg_database_size(current_database())) as size_pretty,
                    current_database() as database_name
            """

            result = self.fetch_one(query)

            if result:
                return {
                    "size_bytes": result[0],
                    "size_pretty": result[1],
                    "database_name": result[2]
                }
            return {}

        except Exception as e:
            logger.error(f"Error getting database size: {e}")
            return {}

    def vacuum_table(self, schema: str, table: str, analyze: bool = True) -> bool:
        """Vacuum a table to reclaim storage"""
        try:
            vacuum_cmd = f"VACUUM {'ANALYZE' if analyze else ''} {schema}.{table}"

            # VACUUM cannot run inside a transaction block
            with self.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                conn.execute(text(vacuum_cmd))

            logger.info(f"Vacuumed table {schema}.{table}")
            return True

        except Exception as e:
            logger.error(f"Error vacuuming {schema}.{table}: {e}")
            return False

    def analyze_table(self, schema: str, table: str) -> bool:
        """Update table statistics"""
        try:
            self.execute_transaction(f"ANALYZE {schema}.{table}")
            logger.info(f"Analyzed table {schema}.{table}")
            return True
        except Exception as e:
            logger.error(f"Error analyzing {schema}.{table}: {e}")
            return False

    def truncate_table(self, schema: str, table: str, cascade: bool = False) -> bool:
        """Truncate a table"""
        try:
            cascade_clause = "CASCADE" if cascade else ""
            self.execute_transaction(f"TRUNCATE TABLE {schema}.{table} {cascade_clause}")
            logger.info(f"Truncated table {schema}.{table}")
            return True
        except Exception as e:
            logger.error(f"Error truncating {schema}.{table}: {e}")
            return False

    def create_index(self, schema: str, table: str, index_name: str,
                     columns: List[str], unique: bool = False,
                     where_clause: Optional[str] = None) -> bool:
        """Create an index on a table"""
        try:
            unique_clause = "UNIQUE" if unique else ""
            column_list = ", ".join(columns)
            where = f"WHERE {where_clause}" if where_clause else ""

            index_sql = f"""
                CREATE {unique_clause} INDEX IF NOT EXISTS {index_name}
                ON {schema}.{table} ({column_list})
                {where}
            """

            self.execute_transaction(index_sql)
            logger.info(f"Created index {index_name} on {schema}.{table}")
            return True

        except Exception as e:
            logger.error(f"Error creating index {index_name}: {e}")
            return False

    def drop_index(self, schema: str, index_name: str) -> bool:
        """Drop an index"""
        try:
            self.execute_transaction(f"DROP INDEX IF EXISTS {schema}.{index_name}")
            logger.info(f"Dropped index {schema}.{index_name}")
            return True
        except Exception as e:
            logger.error(f"Error dropping index {schema}.{index_name}: {e}")
            return False

    def get_active_connections(self) -> List[Dict[str, Any]]:
        """Get active database connections"""
        try:
            query = """
                    SELECT pid, \
                           usename, \
                           application_name, \
                           client_addr, \
                           state, \
                           query_start, \
                           state_change, \
                           query
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                      AND pid != pg_backend_pid()
                    ORDER BY query_start DESC \
                    """

            result = self.execute_query(query)

            connections = []
            for row in result:
                connections.append({
                    "pid": row[0],
                    "username": row[1],
                    "application": row[2],
                    "client_address": str(row[3]) if row[3] else None,
                    "state": row[4],
                    "query_start": row[5],
                    "state_change": row[6],
                    "query": row[7]
                })

            return connections

        except Exception as e:
            logger.error(f"Error getting active connections: {e}")
            return []

    def kill_connection(self, pid: int) -> bool:
        """Terminate a database connection"""
        try:
            self.execute_query("SELECT pg_terminate_backend(:pid)", {"pid": pid})
            logger.info(f"Terminated connection with PID {pid}")
            return True
        except Exception as e:
            logger.error(f"Error terminating connection {pid}: {e}")
            return False

    def backup_table(self, schema: str, table: str, backup_suffix: str = "_backup") -> bool:
        """Create a backup copy of a table"""
        try:
            backup_table = f"{table}{backup_suffix}"

            # Drop backup table if exists
            self.execute_transaction(f"DROP TABLE IF EXISTS {schema}.{backup_table}")

            # Create backup
            self.execute_transaction(
                f"CREATE TABLE {schema}.{backup_table} AS SELECT * FROM {schema}.{table}"
            )

            logger.info(f"Created backup table {schema}.{backup_table}")
            return True

        except Exception as e:
            logger.error(f"Error backing up {schema}.{table}: {e}")
            return False

    def get_table_statistics(self, schema: str, table: str) -> Dict[str, Any]:
        """Get detailed statistics for a table"""
        try:
            stats = {
                "row_count": self.get_table_count(schema, table),
                "size": self.get_table_size(schema, table),
                "columns": len(self.get_table_columns(schema, table)),
                "indexes": len(self.get_table_indexes(schema, table))
            }

            # Get date range for time-series tables
            if self.table_exists(schema, table):
                columns = self.get_table_columns(schema, table)
                timestamp_cols = [col['name'] for col in columns
                                  if 'timestamp' in col['type'].lower()]

                if timestamp_cols:
                    date_query = f"""
                        SELECT 
                            MIN({timestamp_cols[0]})::date as min_date,
                            MAX({timestamp_cols[0]})::date as max_date,
                            MAX({timestamp_cols[0]}) - MIN({timestamp_cols[0]}) as date_range
                        FROM {schema}.{table}
                    """

                    result = self.fetch_one(date_query)
                    if result and result[0]:
                        stats["date_range"] = {
                            "min_date": result[0],
                            "max_date": result[1],
                            "range_days": result[2].days if result[2] else 0
                        }

            return stats

        except Exception as e:
            logger.error(f"Error getting statistics for {schema}.{table}: {e}")
            return {}

    def explain_query(self, query: str, params: Optional[dict] = None,
                      analyze: bool = False) -> List[str]:
        """Get query execution plan"""
        try:
            explain_prefix = "EXPLAIN (ANALYZE, BUFFERS)" if analyze else "EXPLAIN"
            explain_query = f"{explain_prefix} {query}"

            result = self.execute_query(explain_query, params)
            return [row[0] for row in result]

        except Exception as e:
            logger.error(f"Error explaining query: {e}")
            return []

    def get_slow_queries(self, duration_ms: int = 1000) -> List[Dict[str, Any]]:
        """Get queries running longer than specified duration"""
        try:
            query = """
                    SELECT pid, \
                           usename, \
                           application_name, \
                           state, \
                           query_start, \
                           EXTRACT(EPOCH FROM (NOW() - query_start)) * 1000 as duration_ms, \
                           query
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                      AND state = 'active'
                      AND pid != pg_backend_pid()
                      AND EXTRACT(EPOCH FROM (NOW() - query_start)) * 1000 > :duration_ms
                    ORDER BY duration_ms DESC \
                    """

            result = self.execute_query(query, {"duration_ms": duration_ms})

            slow_queries = []
            for row in result:
                slow_queries.append({
                    "pid": row[0],
                    "username": row[1],
                    "application": row[2],
                    "state": row[3],
                    "query_start": row[4],
                    "duration_ms": row[5],
                    "query": row[6]
                })

            return slow_queries

        except Exception as e:
            logger.error(f"Error getting slow queries: {e}")
            return []