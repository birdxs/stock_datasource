"""Schema management utilities for dynamic table creation and evolution."""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import pandas as pd

from stock_datasource.models.database import db_client
from stock_datasource.models.schemas import (
    TableSchema, ColumnDefinition, TableType,
    PREDEFINED_SCHEMAS, META_SCHEMA_CATALOG_SCHEMA
)
from stock_datasource.config.settings import settings

logger = logging.getLogger(__name__)


class SchemaManager:
    """Manages dynamic schema creation and evolution."""
    
    def __init__(self):
        self.db = db_client
    
    def create_table_from_schema(self, schema: TableSchema) -> None:
        """Create table from schema definition."""
        if self.db.table_exists(schema.table_name):
            logger.info(f"Table {schema.table_name} already exists")
            return
        
        # 预处理：确保分区键和排序键合法
        schema = self._sanitize_schema(schema)
        
        create_sql = self._build_create_table_sql(schema)
        
        try:
            self.db.create_table(create_sql)
            self._log_schema_change(schema.table_name, "CREATE_TABLE", create_sql)
            logger.info(f"Created table {schema.table_name}")
        except Exception as e:
            logger.error(f"Failed to create table {schema.table_name}: {e}")
            logger.error(f"SQL: {create_sql}")
            raise
    
    def _sanitize_schema(self, schema: TableSchema) -> TableSchema:
        """清理 schema，确保 ClickHouse 兼容性"""
        # 检查分区键非确定性
        if schema.partition_by and "now()" in str(schema.partition_by):
            logger.warning(f"Table {schema.table_name}: Removing non-deterministic partition key")
            # 尝试使用已有的日期列
            date_cols = [c.name for c in schema.columns 
                        if 'date' in c.name.lower() or c.data_type in ['Date', 'DateTime']]
            if date_cols:
                schema.partition_by = f"toYYYYMM({date_cols[0]})"
            else:
                schema.partition_by = None
        
        # 确保排序键列不是 Nullable
        if schema.order_by:
            col_map = {c.name: c for c in schema.columns}
            for col_name in schema.order_by:
                if col_name in col_map:
                    col = col_map[col_name]
                    if col.data_type.startswith("Nullable("):
                        # 提取内部类型，改为非 Nullable
                        inner_type = col.data_type[9:-1]  # 移除 "Nullable(" 和 ")"
                        col.data_type = inner_type
                        col.nullable = False
                        logger.info(f"Changed {col_name} to non-nullable for ORDER BY")
        
        return schema
    
    def _build_create_table_sql(self, schema: TableSchema) -> str:
        """Build CREATE TABLE SQL from schema definition."""
        columns_sql = []
        for col in schema.columns:
            col_sql = f"{col.name} {col.data_type}"
            if col.default_value:
                col_sql += f" DEFAULT {col.default_value}"
            if col.comment:
                col_sql += f" COMMENT '{col.comment}'"
            columns_sql.append(col_sql)
        
        # Build engine SQL
        if schema.engine_params:
            engine_sql = f"{schema.engine}({', '.join(str(p) for p in schema.engine_params)})"
        else:
            engine_sql = schema.engine
        
        sql_parts = [
            f"CREATE TABLE IF NOT EXISTS {schema.table_name} (",
            ",\n".join(columns_sql),
            f") ENGINE = {engine_sql}"
        ]
        
        if schema.partition_by:
            sql_parts.append(f"PARTITION BY {schema.partition_by}")
        
        if schema.order_by:
            order_by_str = ", ".join(f"{col}" for col in schema.order_by)
            sql_parts.append(f"ORDER BY ({order_by_str})")
        
        # 检查是否需要允许 Nullable 排序键
        order_by_set = set(schema.order_by or [])
        has_nullable_key = any(
            col.name in order_by_set and col.nullable 
            for col in schema.columns
        )
        if has_nullable_key:
            sql_parts.append("SETTINGS allow_nullable_key = 1")
        
        if schema.comment:
            sql_parts.append(f"COMMENT '{schema.comment}'")
        
        return "\n".join(sql_parts)
    
    def sync_schema_from_api(self, table_name: str, api_data: pd.DataFrame, 
                           api_name: str) -> bool:
        """
        Sync table schema with API data (Schema-on-API).
        Returns True if schema was modified.
        """
        if not self.db.table_exists(table_name):
            # Create new table with inferred schema
            schema = self._infer_schema_from_data(table_name, api_data, api_name)
            self.create_table_from_schema(schema)
            return True
        
        # ... 其余逻辑保持不变 ...
    
    def _infer_schema_from_data(self, table_name: str, data: pd.DataFrame, 
                               api_name: str) -> TableSchema:
        """Infer table schema from pandas DataFrame."""
        columns = []
        
        # Add data columns
        for col_name in data.columns:
            data_type = self._infer_clickhouse_type(data[col_name])
            columns.append(ColumnDefinition(
                name=col_name,
                data_type=data_type,
                nullable=True
            ))
        
        # Add system columns - 使用确定性默认值
        columns.extend([
            ColumnDefinition(
                name="version",
                data_type="UInt32",
                nullable=False,
                default_value="0"
            ),
            ColumnDefinition(
                name="_ingested_at",
                data_type="DateTime",
                nullable=False,
                default_value="1970-01-01 00:00:00"
            )
        ])
        
        # Determine partition and order keys
        if 'trade_date' in data.columns:
            partition_by = "toYYYYMM(trade_date)"
            order_by = []
            if 'ts_code' in data.columns:
                # 确保 ts_code 不是 Nullable
                order_by.append('ts_code')
            order_by.append('trade_date')
        else:
            # 无 trade_date：使用 version 分区（转换为日期）
            partition_by = "toYYYYMM(toDate(version))"
            order_by = ["version"]
        
        # 确保排序键列非 Nullable
        col_map = {c.name: c for c in columns}
        for col_name in order_by:
            if col_name in col_map:
                col = col_map[col_name]
                if col.data_type.startswith("Nullable("):
                    col.data_type = col.data_type[9:-1]
                    col.nullable = False
        
        return TableSchema(
            table_name=table_name,
            table_type=TableType.ODS,
            columns=columns,
            partition_by=partition_by,
            order_by=order_by,
            engine="ReplacingMergeTree",
            engine_params=["version"],
            comment=f"ODS table for {api_name} API data"
        )
    
    def _infer_clickhouse_type(self, series: pd.Series) -> str:
        """Infer ClickHouse data type from pandas Series."""
        if pd.api.types.is_integer_dtype(series):
            return "Nullable(Int64)"
        elif pd.api.types.is_float_dtype(series):
            return "Nullable(Float64)"
        elif pd.api.types.is_bool_dtype(series):
            return "Nullable(Bool)"
        elif pd.api.types.is_datetime64_dtype(series):
            return "Nullable(DateTime)"
        elif pd.api.types.is_string_dtype(series):
            if series.name and any(keyword in series.name.lower() for keyword in ['code', 'symbol', 'ticker']):
                return "LowCardinality(String)"
            return "Nullable(String)"
        else:
            return "Nullable(String)"
    
    # ... _check_type_mismatches, _is_widening_conversion 保持不变 ...
    
    def _log_schema_change(self, table_name: str, change_type: str, 
                          change_details: str) -> None:
        """Log schema change to metadata table."""
        try:
            if not self.db.table_exists("meta_schema_changelog"):
                self._create_schema_changelog_table()
            
            query = """
            INSERT INTO meta_schema_changelog 
            (table_name, change_type, change_details, created_at)
            VALUES
            """
            self.db.execute(query, {
                "table_name": table_name,
                "change_type": change_type,
                "change_details": change_details[:1000],  # 限制长度
                "created_at": datetime.now()
            })
        except Exception as e:
            logger.error(f"Failed to log schema change: {e}")
    
    def _create_schema_changelog_table(self) -> None:
        """Create schema changelog table."""
        create_sql = """
        CREATE TABLE IF NOT EXISTS meta_schema_changelog (
            id UInt64 DEFAULT generateUUIDv4(),
            table_name String,
            change_type String,
            change_details String,
            created_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(created_at)
        ORDER BY (created_at, table_name)
        SETTINGS allow_nullable_key = 1
        """
        self.db.create_table(create_sql)
    
    # ... create_predefined_tables, get_schema_summary 保持不变 ...


def dict_to_schema(schema_dict: Dict[str, Any]) -> TableSchema:
    """Convert a plugin schema dict (schema.json) to a TableSchema object."""
    columns: List[ColumnDefinition] = []
    
    # 验证
    if "table_name" not in schema_dict:
        raise ValueError("Schema missing table_name")
    if "columns" not in schema_dict or not schema_dict["columns"]:
        raise ValueError(f"Schema {schema_dict.get('table_name')} missing columns")
    
    for col_dict in schema_dict.get("columns", []):
        if "name" not in col_dict or "data_type" not in col_dict:
            raise ValueError(f"Invalid column definition in {schema_dict['table_name']}: {col_dict}")
            
        default_value = col_dict.get("default") or col_dict.get("default_value")

        columns.append(
            ColumnDefinition(
                name=col_dict["name"],
                data_type=col_dict["data_type"],
                nullable=col_dict.get("nullable", True),
                default_value=default_value,
                comment=col_dict.get("comment"),
            )
        )

    table_type_str = schema_dict.get("table_type", "ods")
    try:
        table_type = TableType(table_type_str)
    except ValueError:
        table_type = TableType.ODS

    # 处理非确定性分区键
    partition_by = schema_dict.get("partition_by")
    if partition_by and "now()" in str(partition_by):
        logger.warning(f"Removing non-deterministic partition_by from {schema_dict['table_name']}")
        partition_by = None

    return TableSchema(
        table_name=schema_dict["table_name"],
        table_type=table_type,
        columns=columns,
        partition_by=partition_by,
        order_by=schema_dict.get("order_by", []),
        engine=schema_dict.get("engine", "ReplacingMergeTree"),
        engine_params=schema_dict.get("engine_params"),
        comment=schema_dict.get("comment"),
    )


# Global schema manager instance
schema_manager = SchemaManager()
