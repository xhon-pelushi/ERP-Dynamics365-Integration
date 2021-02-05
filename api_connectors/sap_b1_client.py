"""
SAP Business One API Client
Connects to SAP B1 Service Layer API
"""

import requests
import json
from typing import Dict, List, Optional
from datetime import datetime

class SAPB1Client:
    def __init__(self, base_url: str, username: str, password: str, database: str):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password
        self.database = database
        self.session = requests.Session()
        self.session_id = None
        
    def login(self) -> bool:
        """Authenticate with SAP B1 Service Layer"""
        login_url = f"{self.base_url}/Login"
        login_data = {
            "UserName": self.username,
            "Password": self.password,
            "CompanyDB": self.database
        }
        
        try:
            response = self.session.post(login_url, json=login_data)
            if response.status_code == 200:
                self.session_id = response.cookies.get('B1SESSION')
                self.session.headers.update({
                    'B1SESSION': self.session_id,
                    'Content-Type': 'application/json'
                })
                return True
            else:
                print(f"Login failed: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"Login error: {e}")
            return False
    
    def logout(self) -> bool:
        """Logout from SAP B1 Service Layer"""
        if self.session_id:
            logout_url = f"{self.base_url}/Logout"
            try:
                self.session.post(logout_url)
                return True
            except Exception as e:
                print(f"Logout error: {e}")
                return False
        return True
    
    def get_orders(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict]:
        """Retrieve sales orders from SAP B1"""
        orders_url = f"{self.base_url}/Orders"
        
        # Build query string
        query_params = []
        if start_date:
            query_params.append(f"$filter=DocDate ge '{start_date}'")
        if end_date:
            if query_params:
                query_params[0] += f" and DocDate le '{end_date}'"
            else:
                query_params.append(f"$filter=DocDate le '{end_date}'")
        
        query_string = '&'.join(query_params) if query_params else ''
        
        try:
            response = self.session.get(f"{orders_url}?{query_string}")
            if response.status_code == 200:
                data = response.json()
                return data.get('value', [])
            else:
                print(f"Failed to retrieve orders: {response.status_code}")
                return []
        except Exception as e:
            print(f"Error retrieving orders: {e}")
            return []
    
    def get_customers(self) -> List[Dict]:
        """Retrieve customer master data"""
        customers_url = f"{self.base_url}/BusinessPartners?$filter=CardType eq 'C'"
        
        try:
            response = self.session.get(customers_url)
            if response.status_code == 200:
                data = response.json()
                return data.get('value', [])
            else:
                print(f"Failed to retrieve customers: {response.status_code}")
                return []
        except Exception as e:
            print(f"Error retrieving customers: {e}")
            return []
    
    def get_products(self) -> List[Dict]:
        """Retrieve product master data"""
        products_url = f"{self.base_url}/Items"
        
        try:
            response = self.session.get(products_url)
            if response.status_code == 200:
                data = response.json()
                return data.get('value', [])
            else:
                print(f"Failed to retrieve products: {response.status_code}")
                return []
        except Exception as e:
            print(f"Error retrieving products: {e}")
            return []




















































