"""TuShare rt_etf_min (ETF实时分钟K线) query service."""

from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

from stock_datasource.core.base_service import BaseService, query_method, QueryParam


class RtEtfMinService(BaseService):
    """Query service for ETF real-time minute data."""
    
    table_name = "ods_rt_etf_min"
    
    @query_method(
        description="获取ETF实时分钟K线数据",
        params=[
            QueryParam(name="ts_code", type="str", required=True, description="ETF代码"),
            QueryParam(name="freq", type="str", required=False, description="K线频率(1MIN/5MIN/15MIN/30MIN/60MIN)"),
            QueryParam(name="start_time", type="str", required=False, description="开始时间"),
            QueryParam(name="end_time", type="str", required=False, description="结束时间"),
            QueryParam(name="limit", type="int", required=False, description="返回记录数限制")
        ]
    )
    def get_etf_mins(
        self,
        ts_code: str,
        freq: str = "1MIN",
        start_time: Optional[str] = None,
        end_time: Optional[str] = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Get ETF minute K-line data."""
        params = {
            "ts_code": ts_code,
            "freq": freq,
            "limit": int(limit)
        }
        
        time_filter = ""
        if start_time:
            time_filter += " AND trade_time >= %(start_time)s"
            params["start_time"] = start_time
        if end_time:
            time_filter += " AND trade_time <= %(end_time)s"
            params["end_time"] = end_time
        
        sql = f"""
            SELECT ts_code, freq, trade_time, open, close, high, low, vol, amount
            FROM {self.table_name}
            WHERE ts_code = %(ts_code)s AND freq = %(freq)s
            {time_filter}
            ORDER BY trade_time DESC
            LIMIT %(limit)s
        """
        result = self.client.execute(sql, params)
        
        columns = ["ts_code", "freq", "trade_time", "open", "close", "high", "low", "vol", "amount"]
        return [dict(zip(columns, row)) for row in result]
    
    @query_method(
        description="获取ETF最新N条分钟数据",
        params=[
            QueryParam(name="ts_code", type="str", required=True, description="ETF代码"),
            QueryParam(name="freq", type="str", required=False, description="K线频率"),
            QueryParam(name="count", type="int", required=False, description="返回条数")
        ]
    )
    def get_latest_etf_mins(
        self,
        ts_code: str,
        freq: str = "1MIN",
        count: int = 100
    ) -> List[Dict[str, Any]]:
        """Get latest N ETF minute records."""
        sql = f"""
            SELECT ts_code, freq, trade_time, open, close, high, low, vol, amount
            FROM {self.table_name}
            WHERE ts_code = %(ts_code)s AND freq = %(freq)s
            ORDER BY trade_time DESC
            LIMIT %(count)s
        """
        result = self.client.execute(sql, {
            "ts_code": ts_code,
            "freq": freq,
            "count": int(count)
        })
        
        columns = ["ts_code", "freq", "trade_time", "open", "close", "high", "low", "vol", "amount"]
        return [dict(zip(columns, row)) for row in result]
    
    @query_method(
        description="获取ETF分钟数据统计摘要",
        params=[
            QueryParam(name="ts_code", type="str", required=False, description="ETF代码(可选)")
        ]
    )
    def get_etf_mins_summary(self, ts_code: Optional[str] = None) -> Dict[str, Any]:
        """Get ETF minute data summary."""
        ts_filter = ""
        params = {}
        
        if ts_code:
            ts_filter = "WHERE ts_code = %(ts_code)s"
            params["ts_code"] = ts_code
        
        sql = f"""
            SELECT 
                count() as total_records,
                count(DISTINCT ts_code) as etf_count,
                min(trade_time) as earliest_time,
                max(trade_time) as latest_time,
                countIf(freq = '1MIN') as mins_1,
                countIf(freq = '5MIN') as mins_5,
                countIf(freq = '15MIN') as mins_15,
                countIf(freq = '30MIN') as mins_30,
                countIf(freq = '60MIN') as mins_60
            FROM {self.table_name}
            {ts_filter}
        """
        result = self.client.execute(sql, params)
        
        if result:
            row = result[0]
            return {
                "total_records": row[0],
                "etf_count": row[1],
                "earliest_time": row[2],
                "latest_time": row[3],
                "by_freq": {
                    "1MIN": row[4],
                    "5MIN": row[5],
                    "15MIN": row[6],
                    "30MIN": row[7],
                    "60MIN": row[8]
                }
            }
        return {}
