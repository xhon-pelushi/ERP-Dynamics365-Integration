"""
Synchronization Engine for Orders
Syncs sales orders from SAP B1 to Dynamics 365 CRM
"""

from datetime import datetime, timedelta
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api_connectors.sap_b1_client import SAPB1Client
from api_connectors.dynamics365_client import Dynamics365Client
import pyodbc

class OrderSyncEngine:
    def __init__(self, sap_config: dict, dynamics_config: dict, sql_config: dict):
        self.sap_client = SAPB1Client(**sap_config)
        self.dynamics_client = Dynamics365Client(**dynamics_config)
        self.sql_config = sql_config
        self.sync_stats = {
            'total_orders': 0,
            'synced': 0,
            'failed': 0,
            'skipped': 0
        }
    
    def get_sql_connection(self):
        """Get SQL Server connection"""
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.sql_config['server']};"
            f"DATABASE={self.sql_config['database']};"
            f"UID={self.sql_config['username']};"
            f"PWD={self.sql_config['password']}"
        )
        return pyodbc.connect(conn_str)
    
    def sync_orders(self, start_date: str = None, end_date: str = None) -> dict:
        """Main synchronization method"""
        print(f"Starting order synchronization: {start_date} to {end_date}")
        
        # Authenticate
        if not self.sap_client.login():
            print("Failed to authenticate with SAP B1")
            return self.sync_stats
        
        if not self.dynamics_client.authenticate():
            print("Failed to authenticate with Dynamics 365")
            self.sap_client.logout()
            return self.sync_stats
        
        # Get orders from SAP B1
        orders = self.sap_client.get_orders(start_date, end_date)
        self.sync_stats['total_orders'] = len(orders)
        
        print(f"Retrieved {len(orders)} orders from SAP B1")
        
        # Process each order
        for order in orders:
            try:
                # Check if order already synced
                if self._is_order_synced(order['DocEntry']):
                    print(f"Order {order['DocEntry']} already synced, skipping")
                    self.sync_stats['skipped'] += 1
                    continue
                
                # Transform SAP order to Dynamics 365 format
                opportunity_data = self._transform_order_to_opportunity(order)
                
                # Create opportunity in Dynamics 365
                opportunity_id = self.dynamics_client.create_opportunity(opportunity_data)
                
                if opportunity_id:
                    # Record sync in staging table
                    self._record_sync(order['DocEntry'], opportunity_id, 'success')
                    self.sync_stats['synced'] += 1
                    print(f"Successfully synced order {order['DocEntry']} -> opportunity {opportunity_id}")
                else:
                    self._record_sync(order['DocEntry'], None, 'failed')
                    self.sync_stats['failed'] += 1
                    print(f"Failed to sync order {order['DocEntry']}")
                    
            except Exception as e:
                print(f"Error processing order {order.get('DocEntry', 'unknown')}: {e}")
                self.sync_stats['failed'] += 1
        
        # Logout
        self.sap_client.logout()
        
        print(f"Synchronization completed: {self.sync_stats}")
        return self.sync_stats
    
    def _transform_order_to_opportunity(self, sap_order: dict) -> dict:
        """Transform SAP B1 order to Dynamics 365 opportunity format"""
        return {
            'name': f"SAP Order {sap_order.get('DocNum', '')}",
            'estimatedvalue': float(sap_order.get('DocTotal', 0)),
            'estimatedclosedate': sap_order.get('DocDueDate', datetime.now().isoformat()),
            'description': f"Order from SAP B1 - DocEntry: {sap_order.get('DocEntry')}",
            'statuscode': 1,  # In Progress
            'customerid': self._get_dynamics_account_id(sap_order.get('CardCode', ''))
        }
    
    def _get_dynamics_account_id(self, sap_card_code: str) -> str:
        """Get Dynamics 365 account ID for SAP customer code"""
        # Query Dynamics 365 for account with matching external ID
        accounts = self.dynamics_client.get_accounts(f"accountnumber eq '{sap_card_code}'")
        if accounts:
            return accounts[0].get('accountid', '')
        return None
    
    def _is_order_synced(self, sap_doc_entry: int) -> bool:
        """Check if order has already been synced"""
        try:
            conn = self.get_sql_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM SyncLog WHERE SAPDocEntry = ? AND Status = 'success'",
                sap_doc_entry
            )
            count = cursor.fetchone()[0]
            conn.close()
            return count > 0
        except Exception as e:
            print(f"Error checking sync status: {e}")
            return False
    
    def _record_sync(self, sap_doc_entry: int, dynamics_id: str, status: str):
        """Record synchronization in staging table"""
        try:
            conn = self.get_sql_connection()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO SyncLog (SAPDocEntry, DynamicsID, Status, SyncDate)
                VALUES (?, ?, ?, ?)
            """, sap_doc_entry, dynamics_id, status, datetime.now())
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error recording sync: {e}")

if __name__ == '__main__':
    # Configuration (should be in config file)
    sap_config = {
        'base_url': 'https://sapb1-server:50000/b1s/v1',
        'username': 'sap_user',
        'password': 'sap_password',
        'database': 'SAP_DB'
    }
    
    dynamics_config = {
        'tenant_id': 'your-tenant-id',
        'client_id': 'your-client-id',
        'client_secret': 'your-client-secret',
        'dynamics_url': 'https://yourorg.crm.dynamics.com'
    }
    
    sql_config = {
        'server': 'localhost',
        'database': 'ERPIntegration',
        'username': 'sa',
        'password': 'password'
    }
    
    # Run synchronization
    sync_engine = OrderSyncEngine(sap_config, dynamics_config, sql_config)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    stats = sync_engine.sync_orders(start_date, end_date)
    print(f"Sync Statistics: {stats}")


















































