import json
import os
import sys

import requests
from dotenv import load_dotenv

# Load environment variables from .creds/.env
load_dotenv('.creds/.env')


def main():
    # Get the contents of the environment variable SQUARE_API_KEY into a string
    square_api_key = os.environ.get('SQUARE_API_KEY')
    if not square_api_key:
        raise ValueError('No SQUARE_API_KEY environment variable set')

    # Set the headers for the request
    headers = {
        'Authorization': f'Bearer {square_api_key}',
        'Content-Type': 'application/json',
        'Square-Version': '2024-12-18',
    }

    # get the item ID from the first command line argument
    item_id = sys.argv[1]
    if not item_id:
        raise ValueError('No item ID provided')

    # Set the URL for the Square Catalog API
    url = f'https://connect.squareup.com/v2/catalog/object/{item_id}'

    # Make the request
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()

    # Get the cursor from the response
    cursor = data['cursor']
    while cursor:
        process_items(data['objects'])
        response = requests.get(url, headers=headers, params={'cursor': cursor})
        response.raise_for_status()
        data = response.json()
        cursor = data.get('cursor', None)

    process_items(data['objects'])


def process_items(items):
    if not items or len(items) == 0:
        print('No items')
        return

    # Create a directory named 'items' if it doesn't exist
    if not os.path.exists('items'):
        os.makedirs('items')

    for item in items:
        # Write the item to a JSON file named with the item's ID
        with open(f'items/{item["id"]}.json', 'w') as f:
            json.dump(item, f, indent=4)
        print(f'Wrote {item["id"]}.json')


if __name__ == "__main__":
    main()
