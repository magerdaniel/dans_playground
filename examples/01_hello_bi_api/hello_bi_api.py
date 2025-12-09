"""
Hello BI API - A simple example of connecting to a BI platform API

This script demonstrates:
- Loading environment variables
- API authentication
- Making HTTP requests
- Parsing JSON responses
- Basic error handling
"""

import os
import requests
from dotenv import load_dotenv


def load_config():
    """Load configuration from environment variables."""
    load_dotenv()
    
    config = {
        'api_key': os.getenv('API_KEY'),
        'base_url': os.getenv('API_BASE_URL', 'https://api.example.com'),
    }
    
    # Validate required configuration
    if not config['api_key']:
        raise ValueError("API_KEY not found in environment variables")
    
    return config


def get_api_client(config):
    """Create and return an API client session."""
    session = requests.Session()
    session.headers.update({
        'Authorization': f'Bearer {config["api_key"]}',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    })
    return session


def fetch_resources(client, base_url, resource_type='reports'):
    """
    Fetch resources from the API.
    
    Args:
        client: requests.Session object
        base_url: Base URL for the API
        resource_type: Type of resource to fetch (e.g., 'reports', 'dashboards')
    
    Returns:
        List of resources or None if request fails
    """
    url = f"{base_url}/{resource_type}"
    
    try:
        response = client.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text}")
        return None
    
    except requests.exceptions.ConnectionError:
        print("Connection Error: Unable to connect to the API")
        return None
    
    except requests.exceptions.Timeout:
        print("Timeout Error: Request took too long")
        return None
    
    except requests.exceptions.RequestException as e:
        print(f"Request Error: {e}")
        return None


def display_resources(resources, resource_type):
    """Display resources in a readable format."""
    if not resources:
        print(f"No {resource_type} found or error occurred.")
        return
    
    print(f"\n{'='*60}")
    print(f"Available {resource_type.title()}")
    print(f"{'='*60}\n")
    
    # Handle different response formats
    if isinstance(resources, dict):
        items = resources.get('data', resources.get('items', [resources]))
    else:
        items = resources
    
    for idx, item in enumerate(items, 1):
        if isinstance(item, dict):
            name = item.get('name', item.get('title', 'Unknown'))
            id_val = item.get('id', 'N/A')
            print(f"{idx}. {name}")
            print(f"   ID: {id_val}")
        else:
            print(f"{idx}. {item}")
        print()


def main():
    """Main function to run the example."""
    print("Hello BI API - Example Script")
    print("-" * 60)
    
    try:
        # Load configuration
        print("\n1. Loading configuration...")
        config = load_config()
        print(f"   Base URL: {config['base_url']}")
        print("   API Key: ****" + config['api_key'][-4:] if len(config['api_key']) > 4 else "****")
        
        # Create API client
        print("\n2. Creating API client...")
        client = get_api_client(config)
        print("   Client created successfully")
        
        # Fetch resources
        print("\n3. Fetching resources from API...")
        resources = fetch_resources(client, config['base_url'])
        
        # Display results
        print("\n4. Displaying results...")
        display_resources(resources, 'reports')
        
        print("\n" + "="*60)
        print("Example completed successfully!")
        print("="*60 + "\n")
        
    except ValueError as e:
        print(f"\nConfiguration Error: {e}")
        print("\nPlease create a .env file with:")
        print("  API_KEY=your_api_key_here")
        print("  API_BASE_URL=https://api.example.com")
    
    except Exception as e:
        print(f"\nUnexpected Error: {e}")


if __name__ == "__main__":
    main()
