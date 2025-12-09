# Example 1: Hello BI API

A simple starter project demonstrating how to connect to a Business Intelligence API and retrieve basic information.

## Purpose

This example teaches:
- API authentication basics
- Making HTTP requests
- Parsing JSON responses
- Error handling
- Environment variable usage

## What You'll Build

A simple script that:
1. Authenticates with a BI platform API
2. Retrieves a list of available resources (reports, dashboards, etc.)
3. Displays the results in a readable format

## Prerequisites

```bash
pip install requests python-dotenv
```

## Setup

1. Create a `.env` file in the project root:
```
API_KEY=your_api_key_here
API_BASE_URL=https://api.example.com
```

2. Run the example:
```bash
python hello_bi_api.py
```

## Code Structure

- `hello_bi_api.py` - Main script with API connection logic
- `config.py` - Configuration and environment variable loading
- `requirements.txt` - Python dependencies

## Learning Points

### API Authentication
Most BI platforms use one of these methods:
- API keys in headers
- OAuth tokens
- Username/password combinations

### Making Requests
```python
import requests

headers = {
    'Authorization': f'Bearer {api_key}',
    'Content-Type': 'application/json'
}

response = requests.get(url, headers=headers)
```

### Error Handling
Always check response status and handle errors:
```python
if response.status_code == 200:
    data = response.json()
else:
    print(f"Error: {response.status_code}")
```

## Next Steps

After completing this example:
1. Try different API endpoints
2. Add filtering and sorting parameters
3. Parse and transform the returned data
4. Save results to a file
5. Add logging for debugging

## Common Issues

- **401 Unauthorized**: Check your API key
- **404 Not Found**: Verify the endpoint URL
- **Rate limiting**: Add delays between requests
- **Connection errors**: Check network connectivity

## Extension Ideas

- Add command-line arguments for flexibility
- Implement caching to reduce API calls
- Create a simple CLI interface
- Export results to CSV or JSON
- Add retry logic for failed requests
