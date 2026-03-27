"""TuShare stock_company query service."""

from typing import Any, Dict, List, Optional
from datetime import datetime

from stock_datasource.core.base_service import BaseService, query_method, QueryParam


class StockCompanyService(BaseService):
    """Query service for stock company information."""
    
    table_name = "ods_stock_company"
    
    @query_method(
        description="获取上市公司基本信息",
        params=[
            QueryParam(name="ts_code", type="str", required=False, description="股票代码"),
            QueryParam(name="exchange", type="str", required=False, description="交易所代码"),
            QueryParam(name="limit", type="int", required=False, description="返回记录数限制")
        ]
    )
    def get_stock_company(
        self,
        ts_code: Optional[str] = None,
        exchange: Optional[str] = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Get stock company basic information."""
        params = {"limit": int(limit)}
        filters = []
        
        if ts_code:
            filters.append("ts_code = %(ts_code)s")
            params["ts_code"] = ts_code
        if exchange:
            filters.append("exchange = %(exchange)s")
            params["exchange"] = exchange
        
        where_clause = " AND ".join(filters) if filters else "1=1"
        
        sql = f"""
            SELECT ts_code, com_name, com_id, exchange, chairman, manager, secretary,
                   reg_capital, setup_date, province, city, introduction, website,
                   email, office, employees, main_business, business_scope
            FROM {self.table_name}
            WHERE {where_clause}
            LIMIT %(limit)s
        """
        result = self.client.execute(sql, params)
        
        columns = ["ts_code", "com_name", "com_id", "exchange", "chairman", "manager",
                   "secretary", "reg_capital", "setup_date", "province", "city",
                   "introduction", "website", "email", "office", "employees",
                   "main_business", "business_scope"]
        return [dict(zip(columns, row)) for row in result]
    
    @query_method(
        description="根据股票代码获取公司信息",
        params=[
            QueryParam(name="ts_code", type="str", required=True, description="股票代码")
        ]
    )
    def get_company_by_code(self, ts_code: str) -> Optional[Dict[str, Any]]:
        """Get company info by stock code."""
        sql = f"""
            SELECT ts_code, com_name, com_id, exchange, chairman, manager, secretary,
                   reg_capital, setup_date, province, city, introduction, website,
                   email, office, employees, main_business, business_scope
            FROM {self.table_name}
            WHERE ts_code = %(ts_code)s
            LIMIT 1
        """
        result = self.client.execute(sql, {"ts_code": ts_code})
        
        if not result:
            return None
        
        columns = ["ts_code", "com_name", "com_id", "exchange", "chairman", "manager",
                   "secretary", "reg_capital", "setup_date", "province", "city",
                   "introduction", "website", "email", "office", "employees",
                   "main_business", "business_scope"]
        return dict(zip(columns, result[0]))
    
    @query_method(
        description="获取指定省份的上市公司列表",
        params=[
            QueryParam(name="province", type="str", required=True, description="省份名称"),
            QueryParam(name="limit", type="int", required=False, description="返回记录数限制")
        ]
    )
    def get_companies_by_province(
        self,
        province: str,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Get companies by province."""
        sql = f"""
            SELECT ts_code, com_name, exchange, city, main_business
            FROM {self.table_name}
            WHERE province = %(province)s
            LIMIT %(limit)s
        """
        result = self.client.execute(sql, {"province": province, "limit": int(limit)})
        
        columns = ["ts_code", "com_name", "exchange", "city", "main_business"]
        return [dict(zip(columns, row)) for row in result]
    
    @query_method(
        description="搜索公司名称或业务",
        params=[
            QueryParam(name="keyword", type="str", required=True, description="搜索关键词"),
            QueryParam(name="limit", type="int", required=False, description="返回记录数限制")
        ]
    )
    def search_companies(
        self,
        keyword: str,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Search companies by keyword."""
        sql = f"""
            SELECT ts_code, com_name, main_business, business_scope
            FROM {self.table_name}
            WHERE com_name LIKE %(keyword)s
               OR main_business LIKE %(keyword)s
               OR business_scope LIKE %(keyword)s
            LIMIT %(limit)s
        """
        result = self.client.execute(sql, {
            "keyword": f"%{keyword}%",
            "limit": int(limit)
        })
        
        columns = ["ts_code", "com_name", "main_business", "business_scope"]
        return [dict(zip(columns, row)) for row in result]
