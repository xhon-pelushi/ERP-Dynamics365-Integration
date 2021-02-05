"""
Dynamics 365 CRM API Client
Connects to Dynamics 365 using Microsoft Graph API and Web API
"""

import requests
import json
from typing import Dict, List, Optional
from msal import ConfidentialClientApplication

class Dynamics365Client:
    def __init__(self, tenant_id: str, client_id: str, client_secret: str, 
                 dynamics_url: str, api_version: str = "v9.2"):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.dynamics_url = dynamics_url.rstrip('/')
        self.api_version = api_version
        self.access_token = None
        self.session = requests.Session()
        
    def authenticate(self) -> bool:
        """Authenticate with Microsoft Azure AD"""
        authority = f"https://login.microsoftonline.com/{self.tenant_id}"
        scope = [f"{self.dynamics_url}/.default"]
        
        app = ConfidentialClientApplication(
            client_id=self.client_id,
            client_credential=self.client_secret,
            authority=authority
        )
        
        try:
            result = app.acquire_token_for_client(scopes=scope)
            if "access_token" in result:
                self.access_token = result["access_token"]
                self.session.headers.update({
                    'Authorization': f'Bearer {self.access_token}',
                    'Content-Type': 'application/json',
                    'OData-MaxVersion': '4.0',
                    'OData-Version': '4.0'
                })
                return True
            else:
                print(f"Authentication failed: {result.get('error_description', 'Unknown error')}")
                return False
        except Exception as e:
            print(f"Authentication error: {e}")
            return False
    
    def create_opportunity(self, opportunity_data: Dict) -> Optional[str]:
        """Create a new opportunity in Dynamics 365"""
        url = f"{self.dynamics_url}/api/data/{self.api_version}/opportunities"
        
        try:
            response = self.session.post(url, json=opportunity_data)
            if response.status_code in [201, 204]:
                # Extract opportunity ID from response headers
                location = response.headers.get('OData-EntityId', '')
                if location:
                    return location.split("'")[1] if "'" in location else None
                return "created"
            else:
                print(f"Failed to create opportunity: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            print(f"Error creating opportunity: {e}")
            return None
    
    def update_opportunity(self, opportunity_id: str, update_data: Dict) -> bool:
        """Update an existing opportunity"""
        url = f"{self.dynamics_url}/api/data/{self.api_version}/opportunities({opportunity_id})"
        
        try:
            response = self.session.patch(url, json=update_data)
            return response.status_code in [204, 200]
        except Exception as e:
            print(f"Error updating opportunity: {e}")
            return False
    
    def get_opportunities(self, filter_query: Optional[str] = None) -> List[Dict]:
        """Retrieve opportunities from Dynamics 365"""
        url = f"{self.dynamics_url}/api/data/{self.api_version}/opportunities"
        
        if filter_query:
            url += f"?$filter={filter_query}"
        
        try:
            response = self.session.get(url)
            if response.status_code == 200:
                data = response.json()
                return data.get('value', [])
            else:
                print(f"Failed to retrieve opportunities: {response.status_code}")
                return []
        except Exception as e:
            print(f"Error retrieving opportunities: {e}")
            return []
    
    def create_account(self, account_data: Dict) -> Optional[str]:
        """Create a new account (customer) in Dynamics 365"""
        url = f"{self.dynamics_url}/api/data/{self.api_version}/accounts"
        
        try:
            response = self.session.post(url, json=account_data)
            if response.status_code in [201, 204]:
                location = response.headers.get('OData-EntityId', '')
                if location:
                    return location.split("'")[1] if "'" in location else None
                return "created"
            else:
                print(f"Failed to create account: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            print(f"Error creating account: {e}")
            return None
    
    def get_accounts(self, filter_query: Optional[str] = None) -> List[Dict]:
        """Retrieve accounts from Dynamics 365"""
        url = f"{self.dynamics_url}/api/data/{self.api_version}/accounts"
        
        if filter_query:
            url += f"?$filter={filter_query}"
        
        try:
            response = self.session.get(url)
            if response.status_code == 200:
                data = response.json()
                return data.get('value', [])
            else:
                print(f"Failed to retrieve accounts: {response.status_code}")
                return []
        except Exception as e:
            print(f"Error retrieving accounts: {e}")
            return []













































