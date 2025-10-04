import sqlite3
import pandas as pd
from typing import Optional, List, Any

class SQLiteManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.connection: Optional[sqlite3.Connection] = None
        self.cursor: Optional[sqlite3.Cursor] = None
    
    def connect(self) -> bool:
        try:
            self.connection = sqlite3.connect(self.db_path)
            self.cursor = self.connection.cursor()
            print('Database connection established')
            return True
        except sqlite3.Error as error:
            print(f'Error connecting to database: {error}')
            return False
    
    def execute_query(self, query: str, params: tuple = None) -> Optional[pd.DataFrame]:
        if not self.connection or not self.cursor:
            print('No database connection. Please connect first.')
            return None
        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)
            
            column_names = [description[0] for description in self.cursor.description]
            
            results = self.cursor.fetchall()
            df = pd.DataFrame(results, columns=column_names)
            
            return df
            
        except sqlite3.Error as error:
            print(f'Error executing query: {error}')
            return None
    
    def execute_many(self, query: str, params_list: List[tuple]) -> bool:
        if not self.connection or not self.cursor:
            print('No database connection. Please connect first.')
            return False
        
        try:
            self.cursor.executemany(query, params_list)
            self.connection.commit()
            print(f'Successfully executed query for {len(params_list)} records')
            return True
            
        except sqlite3.Error as error:
            print(f'Error executing query: {error}')
            self.connection.rollback()
            return False
    
    def get_table_info(self, table_name: str) -> Optional[pd.DataFrame]:
        query = f"PRAGMA table_info({table_name})"
        return self.execute_query(query)
    
    def get_tables(self) -> Optional[pd.DataFrame]:
        query = "SELECT name FROM sqlite_master WHERE type='table'"
        return self.execute_query(query)
    
    def close(self):
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            print('Database connection closed')
        except sqlite3.Error as error:
            print(f'Error closing connection: {error}')
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    
    def get_top_invoices_by_value_range(self, top_n: int, min_value: float, max_value: float) -> Optional[pd.DataFrame]:
        query = """
        SELECT 
            i.InvoiceId,
            i.CustomerId,
            i.InvoiceDate,
            i.BillingAddress,
            i.BillingCity,
            i.BillingCountry,
            ROUND(SUM(il.UnitPrice * il.Quantity), 2) as TotalValue
        FROM Invoice i
        JOIN InvoiceLine il ON i.InvoiceId = il.InvoiceId
        GROUP BY i.InvoiceId, i.CustomerId, i.InvoiceDate, i.BillingAddress, i.BillingCity, i.BillingCountry
        HAVING TotalValue BETWEEN ? AND ?
        ORDER BY TotalValue DESC
        LIMIT ?
        """
        return self.execute_query(query, (min_value, max_value, top_n))

    def get_top_customers_by_invoice_count(self, top_n: int) -> Optional[pd.DataFrame]:
        query = """
        SELECT 
            c.CustomerId,
            c.FirstName,
            c.LastName,
            c.Company,
            c.Country,
            c.Email,
            COUNT(i.InvoiceId) as InvoiceCount
        FROM Customer c
        JOIN Invoice i ON c.CustomerId = i.CustomerId
        GROUP BY c.CustomerId, c.FirstName, c.LastName, c.Company, c.Country, c.Email
        ORDER BY InvoiceCount DESC
        LIMIT ?
        """
        return self.execute_query(query, (top_n,))

    def get_top_customers_by_total_value(self, top_n: int) -> Optional[pd.DataFrame]:
        query = """
        SELECT 
            c.CustomerId,
            c.FirstName,
            c.LastName,
            c.Company,
            c.Country,
            c.Email,
            COUNT(i.InvoiceId) as InvoiceCount,
            ROUND(SUM(il.UnitPrice * il.Quantity), 2) as TotalValue
        FROM Customer c
        JOIN Invoice i ON c.CustomerId = i.CustomerId
        JOIN InvoiceLine il ON i.InvoiceId = il.InvoiceId
        GROUP BY c.CustomerId, c.FirstName, c.LastName, c.Company, c.Country, c.Email
        ORDER BY TotalValue DESC
        LIMIT ?
        """
        return self.execute_query(query, (top_n,))

if __name__ == "__main__":
    with SQLiteManager('databases/Chinook_Sqlite.sqlite') as db:
        print("=== TOP 5 Invoices with value between $10-$20 ===")
        invoices = db.get_top_invoices_by_value_range(5, 10.0, 20.0)
        if invoices is not None:
            print(invoices)
        
        print("\n=== TOP 5 Customers by Invoice Count ===")
        customers_count = db.get_top_customers_by_invoice_count(5)
        if customers_count is not None:
            print(customers_count)
        
        print("\n=== TOP 5 Customers by Total Value ===")
        customers_value = db.get_top_customers_by_total_value(5)
        if customers_value is not None:
            print(customers_value)