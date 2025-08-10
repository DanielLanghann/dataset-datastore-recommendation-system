#!/usr/bin/env python3
"""
Test script to verify the Django login API is working correctly
"""

import requests
import json

# Test login endpoint
login_url = "http://localhost:8000/api/auth/login/"
logout_url = "http://localhost:8000/api/auth/logout/"

def test_login():
    print("Testing login API...")
    
    # Test login with admin credentials
    login_data = {
        "username": "admin",
        "password": "admin123"
    }
    
    try:
        response = requests.post(login_url, json=login_data)
        print(f"Login Response Status: {response.status_code}")
        print(f"Login Response Body: {response.text}")
        
        if response.status_code == 200:
            data = response.json()
            token = data.get('token')
            print(f"✅ Login successful! Token: {token[:20]}...")
            return token
        else:
            print("❌ Login failed!")
            return None
            
    except Exception as e:
        print(f"❌ Login error: {e}")
        return None

def test_logout(token):
    if not token:
        print("❌ No token available for logout test")
        return
        
    print("Testing logout API...")
    
    headers = {
        "Authorization": f"Token {token}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(logout_url, headers=headers)
        print(f"Logout Response Status: {response.status_code}")
        print(f"Logout Response Body: {response.text}")
        
        if response.status_code == 200:
            print("✅ Logout successful!")
        else:
            print("❌ Logout failed!")
            
    except Exception as e:
        print(f"❌ Logout error: {e}")

if __name__ == "__main__":
    print("🔍 Testing Django Authentication API...")
    print("=" * 50)
    
    # Test login
    token = test_login()
    print()
    
    # Test logout
    test_logout(token)
    print()
    
    print("✅ API test completed!")
