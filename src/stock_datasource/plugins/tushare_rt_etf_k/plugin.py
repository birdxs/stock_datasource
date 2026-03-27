"""TuShare rt_etf_k plugin implementation."""

from pathlib import Path
import json
from typing import Any, Dict
from datetime import datetime

import pandas as pd

from stock_datasource.plugins import BasePlugin
from .extractor import RtEtfKExtractor


class TuShareRtEtfKPlugin(BasePlugin):
    @property
    def name(self) -> str:
        return "tushare_rt_etf_k"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "TuShare realtime ETF daily K-line from rt_etf_k API"

    @property
    def api_rate_limit(self) -> int:
        config_file = Path(__file__).parent / "config.json"
        with open(config_file, "r", encoding="utf-8") as f:
            config = json.load(f)
        return config.get("rate_limit", 120)

    def get_schema(self) -> Dict[str, Any]:
        schema_file = Path(__file__).parent / "schema.json"
        with open(schema_file, "r", encoding="utf-8") as f:
            return json.load(f)

    def extract_data(self, **kwargs) -> pd.DataFrame:
        ts_code = kwargs.get("ts_code")
        extractor = RtEtfKExtractor()
        if not ts_code:
            return pd.DataFrame()
        return extractor.extract(ts_code=ts_code)

    def validate_data(self, data: pd.DataFrame) -> bool:
        """Validate ETF K-line data."""
        if data.empty:
            self.logger.warning("Empty ETF K-line data")
            return False
        
        required_columns = ['ts_code', 'trade_time']
        missing_columns = [col for col in required_columns if col not in data.columns]
        
        if missing_columns:
            self.logger.error(f"Missing required columns: {missing_columns}")
            return False
        
        self.logger.info(f"ETF K-line data validation passed for {len(data)} records")
        return True

    def transform_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform ETF K-line data."""
        # Convert numeric columns
        numeric_columns = ['open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_change', 'vol', 'amount']
        for col in numeric_columns:
            if col in data.columns:
                data[col] = pd.to_numeric(data[col], errors='coerce')
        
        # Convert trade_time to datetime
        if 'trade_time' in data.columns:
            data['trade_time'] = pd.to_datetime(data['trade_time'], errors='coerce')
        
        self.logger.info(f"Transformed {len(data)} ETF K-line records")
        return data

    def load_data(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Load ETF K-line data into ClickHouse."""
        if not self.db:
            self.logger.error("Database not initialized")
            return {"status": "failed", "error": "Database not initialized"}
        
        if data.empty:
            self.logger.warning("No data to load")
            return {"status": "no_data", "loaded_records": 0}
        
        try:
            # Add version and timestamp
            data['version'] = int(datetime.now().timestamp())
            data['_ingested_at'] = datetime.now()
            
            # Insert data
            self.db.insert_dataframe('ods_rt_etf_k', data)
            
            self.logger.info(f"Successfully loaded {len(data)} ETF K-line records")
            return {
                "status": "success",
                "loaded_records": len(data),
                "table": "ods_rt_etf_k"
            }
        except Exception as e:
            self.logger.error(f"Failed to load ETF K-line data: {e}")
            return {"status": "failed", "error": str(e)}
