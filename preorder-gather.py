import os
import requests
from datetime import datetime, timezone
import sys
import argparse
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

# Load environment variables from .creds/.env
env_path = os.path.join(os.path.dirname(__file__), '.creds', '.env')
load_dotenv(env_path)

# Get Square API key from environment variable
SQUARE_API_KEY = os.getenv('SQUARE_API_KEY')

if not SQUARE_API_KEY:
    print("Error: SQUARE_API_KEY environment variable is not set")
    sys.exit(1)

# Square API base URL
SQUARE_API_BASE_URL = 'https://connect.squareup.com/v2'

# Configure headers for all requests
headers = {
    'Square-Version': '2025-02-20',  # Current API version
    'Authorization': f'Bearer {SQUARE_API_KEY}',
    'Content-Type': 'application/json'
}

# Add this constant with your location ID
SQUARE_LOCATION_ID = "L0ZE9C5Y6J1B9"

# Add these near the top with other globals
ITEM_QUANTITIES = {}  # Will store {catalog_id: {'name': item_name, 'quantity': count}}
ORDER_STATUS_COUNTS = {
    'NOT_FULFILLED': 0,
    'PICKED_UP': 0,
    'COMPLETED': 0,
    'NO_FULFILLMENT': 0,
    'STILL_SHOPPING': 0
}

def setup_google_sheets():
    # Define the scope for Google Sheets API
    scope = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]

    try:
        # Look for JSON files in .creds directory
        creds_dir = os.path.join(os.path.dirname(__file__), '.creds')
        json_files = [f for f in os.listdir(creds_dir) if f.endswith('.json')]

        if not json_files:
            raise FileNotFoundError("No JSON credential files found in .creds directory")

        # Use the first JSON file found
        creds_path = os.path.join(creds_dir, json_files[0])

        # Load credentials from service account file
        creds = Credentials.from_service_account_file(
            creds_path,
            scopes=scope
        )
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        print(f"Error setting up Google Sheets: {e}")
        sys.exit(1)

def parse_args():
    parser = argparse.ArgumentParser(description='Gather preorders from Square API')
    parser.add_argument(
        '--start-date',
        type=str,
        required=True,
        help='Start date for order search (YYYY-MM-DD format)'
    )
    args = parser.parse_args()

    # Validate date format
    try:
        datetime.strptime(args.start_date, '%Y-%m-%d')
    except ValueError:
        print("Error: Date must be in YYYY-MM-DD format")
        sys.exit(1)

    return args

def search_orders(start_date_str):
    """
    Search orders using the Square API
    Args:
        start_date_str: Date string in YYYY-MM-DD format
    """
    # Convert date string to UTC format
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    start_date_utc = start_date.replace(tzinfo=timezone.utc).isoformat()

    endpoint = f"{SQUARE_API_BASE_URL}/orders/search"

    payload = {
        "query": {
            "filter": {
                "date_time_filter": {
                    "created_at": {
                        "start_at": start_date_utc
                    }
                },
                "states": ["OPEN"],
            }
        },
        "location_ids": [SQUARE_LOCATION_ID]
    }
    print(payload)

    response = requests.post(endpoint, headers=headers, json=payload)
    response.raise_for_status()  # Raise exception for non-200 status codes
    return response.json()

class Order:
    def __init__(self, order_data):
        self.order_data = order_data
        self.fulfillment = self._get_first_fulfillment()

    def _get_first_fulfillment(self):
        """Get the first fulfillment if any exist"""
        fulfillments = self.order_data.get('fulfillments', [])
        return fulfillments[0] if fulfillments else None

    def get_order_id(self):
        return self.order_data.get('id', 'N/A')

    def get_created_at(self):
        return self.order_data.get('created_at', 'N/A')

    def get_amount_due(self):
        return self.order_data.get('net_amount_due_money', {}).get('amount', 0)

    def get_fulfillment_status(self):
        if not self.fulfillment:
            return 'NO_FULFILLMENT'
        return self.fulfillment.get('state', '')

    def get_line_items(self):
        return self.order_data.get('line_items', [])

    def get_customer_info(self):
        if not self.fulfillment:
            return {
                'name': 'N/A',
                'phone': 'N/A',
                'email': 'N/A',
                'pickup_at': 'N/A'
            }

        pickup_details = self.fulfillment.get('pickup_details', {})
        recipient = pickup_details.get('recipient', {})
        if not recipient:
            recipient = self.fulfillment.get('shipment_details', {}).get('recipient', {})

        return {
            'name': recipient.get('display_name', 'N/A'),
            'phone': recipient.get('phone_number', 'N/A'),
            'email': recipient.get('email_address', 'N/A'),
            'pickup_at': pickup_details.get('pickup_at', 'N/A')
        }

    def is_still_shopping(self):
        return self.get_amount_due() > 0

    def is_completed_or_picked_up(self):
        status = self.get_fulfillment_status()
        return status in ['COMPLETED', 'PICKED_UP']

def process_order(order, sheets_client):
    """
    Process an individual order and update Google Sheets
    Args:
        order: Order object containing order information
        sheets_client: Authorized Google Sheets client
    """
    try:
        # Check fulfillment status first
        if not order.fulfillment:
            ORDER_STATUS_COUNTS['NO_FULFILLMENT'] += 1
            print(f"Skipping order {order.get_order_id()}: No fulfillment information")
            return False

        if order.is_still_shopping():
            ORDER_STATUS_COUNTS['STILL_SHOPPING'] += 1
            line_items = order.get_line_items()
            for item in line_items:
                quantity = int(float(item.get('quantity', 0)))
                catalog_id = item.get('catalog_object_id')
                if catalog_id in ITEM_QUANTITIES:
                    ITEM_QUANTITIES[catalog_id]['qty_in_carts'] += quantity
                else:
                    ITEM_QUANTITIES[catalog_id] = {
                        'name': item.get('name', 'Unknown Item'),
                        'quantity': 0,
                        'qty_in_carts': quantity
                    }
            print(f"Skipping order {order.get_order_id()}: Still Shopping ({order.get_amount_due()} in cart)")
            return False

        # Update status counts and process line items only for open orders
        if order.is_completed_or_picked_up():
            status = order.get_fulfillment_status()
            ORDER_STATUS_COUNTS[status] += 1
            print(f"Skipping order {order.get_order_id()}: Already {status}")
            return False

        ORDER_STATUS_COUNTS['NOT_FULFILLED'] += 1
        # Process line items for not fulfilled orders
        for item in order.get_line_items():
            catalog_object_id = item.get('catalog_object_id')
            quantity = int(float(item.get('quantity', 0)))
            name = item.get('name', 'Unknown Item')

            if catalog_object_id:
                if catalog_object_id not in ITEM_QUANTITIES:
                    ITEM_QUANTITIES[catalog_object_id] = {
                        'name': name,
                        'quantity': quantity,
                        'qty_in_carts': 0,
                    }
                else:
                    ITEM_QUANTITIES[catalog_object_id]['quantity'] += quantity

        customer_info = order.get_customer_info()

        print(order.order_data)
        print(f"Processing Order: {order.get_order_id()}")
        print(f"Created: {order.get_created_at()}")
        print(f"Status: {order.get_fulfillment_status()}")
        print(f"Customer: {customer_info}")
        print(f"Items ({len(order.get_line_items())}):")
        for item in order.get_line_items():
            name = item.get('name', 'Unknown Item')
            quantity = int(float(item.get('quantity', 0)))
            print(f"    {quantity}x {name}")

    except Exception as e:
        print(f"Error processing order {order.get_order_id()}: {e}")
        return False

    return True

def main():
    args = parse_args()
    sheets_client = setup_google_sheets()

    try:
        orders_response = search_orders(args.start_date)

        if 'orders' in orders_response:
            orders = [Order(order_data) for order_data in orders_response['orders']]
            print(f"Found {len(orders)} orders")

            successful_orders = sum(1 for order in orders if process_order(order, sheets_client))

            print("\nOrder Status Summary:")
            print(f"Not Fulfilled: {ORDER_STATUS_COUNTS['NOT_FULFILLED']}")
            print(f"Picked Up: {ORDER_STATUS_COUNTS['PICKED_UP']}")
            print(f"Completed/Shipped: {ORDER_STATUS_COUNTS['COMPLETED']}")
            print(f"No Fulfillment Info: {ORDER_STATUS_COUNTS['NO_FULFILLMENT']}")
            print(f"Still Shopping: {ORDER_STATUS_COUNTS['STILL_SHOPPING']}")
            print(f"\nSuccessfully processed {successful_orders} out of {len(orders)} orders")

            print("\nItem Quantities:")
            for item_id, item_data in ITEM_QUANTITIES.items():
                print(f"{item_data['name']} (ID: {item_id}): {item_data['quantity']} in carts: {item_data['qty_in_carts']}")
        else:
            print("No orders found")

    except requests.exceptions.RequestException as e:
        print(f"Error making request to Square API: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
