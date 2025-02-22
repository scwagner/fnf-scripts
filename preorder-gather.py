import os
import requests
from datetime import datetime, timezone
import sys
import argparse
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
import json
import atexit

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
    'STILL_SHOPPING': 0,
    'CANCELED': 0
}

# Add these constants near the top with other globals
MARKET_CATEGORY_ID = os.getenv('MARKET_CATEGORY_ID')
CACHE_FILE_PATH = os.path.join(os.path.dirname(__file__), '.creds', 'catalog_cache.json')
CATALOG_ITEMS_CACHE = {}  # Will be loaded from disk if available

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

        # Get spreadsheet URL from environment variable
        sheet_url = os.getenv('GOOGLE_SHEET_URL')
        if not sheet_url:
            raise ValueError("GOOGLE_SHEET_URL environment variable is not set")

        # Open the spreadsheet and write test value
        spreadsheet = client.open_by_url(sheet_url)
        worksheet = spreadsheet.worksheet('Sheet1')
        worksheet.update('A1', [['Test Value']])
        print("Successfully wrote test value to spreadsheet")

        return client
    except FileNotFoundError as fnf_error:
        print(f"File not found error: {fnf_error}")
    except ValueError as val_error:
        print(f"Value error: {val_error}")
    except Exception as e:
        print(f"General error setting up Google Sheets: {e}")
        print(f"Exception details: {e.__class__.__name__}: {e}")
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

def extract_designer_name(item_name):
    """Extract designer name from item name"""
    if ' by ' in item_name:
        return item_name.split(' by ')[1]
    if '-count' in item_name:
        # Extract designer name from strings like "PRE-ORDER: 18-count Pumpkin Patch Aida - BeStitchMe (Fat Half)"
        import re
        match = re.search(r' - ([^(]+)', item_name)
        if match:
            return match.group(1).strip()
    return 'Unknown Designer'

def extract_item_name(item_name):
    """Extract item name from item name"""
    return_value = item_name.split(' by ')[0] if ' by ' in item_name else item_name
    # Remove 'PRE-ORDER: ' prefix if present
    if return_value.startswith('PRE-ORDER: '):
        return_value = return_value[11:]
    return return_value

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

    def get_state(self):
        return self.order_data.get('state', '')

    def is_cancelled(self):
        return self.get_state() == 'CANCELED'

    def is_still_shopping(self):
        return self.get_amount_due() > 0 or self.get_state() == 'DRAFT'

    def is_completed_or_picked_up(self):
        status = self.get_fulfillment_status()
        return status in ['COMPLETED', 'PICKED_UP']


class OrderItem:
    def __init__(self, item_data):
        self.item_data = item_data

    def get_name(self):
        variation = self.item_data.get('variation_name', '')
        if variation not in ['Regular', '']:
            return f"{self.item_data.get('name')} ({variation})"
        return self.item_data.get('name', '')


def get_catalog_item(item_id):
    """
    Fetch catalog item details from Square API or cache
    Args:
        item_id: The Square catalog item ID
    Returns:
        Catalog item data or None if not found
    """
    if item_id in CATALOG_ITEMS_CACHE:
        return_value = CATALOG_ITEMS_CACHE[item_id]
        if return_value.get('type') == 'ITEM_VARIATION' and len(return_value.get('parent_item', {})) > 0:
            del return_value['parent_item']
            CATALOG_ITEMS_CACHE[item_id] = return_value
        return return_value

    try:
        print(f"Fetching catalog item {item_id}")
        endpoint = f"{SQUARE_API_BASE_URL}/catalog/object/{item_id}?include_category_path_to_root=true&include_related_objects=true"
        response = requests.get(endpoint, headers=headers)
        response.raise_for_status()
        item_data = response.json().get('object', {})
        CATALOG_ITEMS_CACHE[item_id] = item_data
        if item_data.get('type') == 'ITEM_VARIATION':
            parent_item_id = item_data.get('item_variation_data', {}).get('item_id', '')
            if parent_item_id:
                parent_item_data = get_catalog_item(parent_item_id)
        return item_data
    except Exception as e:
        print(f"Error fetching catalog item {item_id}: {e}")
        return None

def is_market_item(item_data):
    """
    Check if an item belongs to the market category
    Args:
        item_data: Catalog item data from Square API
    Returns:
        Boolean indicating if item belongs to market category
    """
    if not item_data:
        return False

    category_ids = []
    if item_data.get('type') == 'ITEM_VARIATION':
        parent_item_id = item_data.get('item_variation_data', {}).get('item_id', '')
        parent_item = get_catalog_item(parent_item_id)
        category_ids = [category.get('id', '') for category in parent_item.get('item_data', {}).get('categories', [])]

    # print(f'category_ids: {category_ids}, MARKET_CATEGORY_ID: {MARKET_CATEGORY_ID}, {MARKET_CATEGORY_ID in category_ids}')
    return MARKET_CATEGORY_ID in category_ids

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
                order_item = OrderItem(item)
                quantity = int(float(item.get('quantity', 0)))
                catalog_id = item.get('catalog_object_id')
                if catalog_id in ITEM_QUANTITIES:
                    ITEM_QUANTITIES[catalog_id]['qty_in_carts'] += quantity
                else:
                    ITEM_QUANTITIES[catalog_id] = {
                        'name': order_item.get_name(),
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

        if order.is_cancelled():
            ORDER_STATUS_COUNTS['CANCELED'] += 1
            print(f"Skipping order {order.get_order_id()}: Already CANCELED")
            return False

        ORDER_STATUS_COUNTS['NOT_FULFILLED'] += 1
        # Process line items for not fulfilled orders
        for item in order.get_line_items():
            order_item = OrderItem(item)
            catalog_object_id = item.get('catalog_object_id')
            quantity = int(float(item.get('quantity', 0)))

            if catalog_object_id:
                if catalog_object_id not in ITEM_QUANTITIES:
                    ITEM_QUANTITIES[catalog_object_id] = {
                        'name': order_item.get_name(),
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

def save_preorder_data(sheets_client):
    """
    Save preorder data to Google Sheets in the 'Pre-Orders' worksheet
    Args:
        sheets_client: Authorized Google Sheets client
    """
    try:
        # Get spreadsheet URL from environment variable
        sheet_url = os.getenv('GOOGLE_SHEET_URL')
        spreadsheet = sheets_client.open_by_url(sheet_url)

        # Try to get the Pre-Orders worksheet, create it if it doesn't exist
        try:
            worksheet = spreadsheet.worksheet('Pre-Orders')
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet('Pre-Orders', 1000, 5)

        # Add summary row at the top with current time and not fulfilled count
        current_time = datetime.now().strftime('%Y-%m-%d %I:%M:%S %p')
        summary = [[
            f"Order Summary: {ORDER_STATUS_COUNTS['NOT_FULFILLED']} orders pending fulfillment | Sheet last updated: {current_time}",
            "", "", "", "", ""
        ]]

        # Set up headers - now in row 2
        headers = [['Item ID', 'Full name', 'Designer', 'Room Number', 'Item', 'Pre-ordered', 'In Carts', 'Total', 'Is Market Item']]

        # Update both summary and headers at once
        worksheet.update(values=summary + headers, range_name='A1:I2')

        # Format summary row
        summary_format = {
            "textFormat": {
                "bold": True,
                "fontSize": 12
            }
        }
        worksheet.format('A1:I1', summary_format)

        # Format header row - now in row 2
        header_format = {
            "backgroundColor": {
                "red": 0.8,
                "green": 0.9,
                "blue": 1.0
            },
            "textFormat": {
                "bold": True
            }
        }
        worksheet.format('A2:I2', header_format)

        # Prepare data for writing - now starting at row 3
        data = []
        for item_id, item_data in ITEM_QUANTITIES.items():
            catalog_item = get_catalog_item(item_id)
            if catalog_item and is_market_item(catalog_item):
                data.append([
                    item_id,
                    item_data['name'],
                    extract_designer_name(item_data['name']),
                    'room_number_placeholder',
                    extract_item_name(item_data['name']),
                    item_data['quantity'],
                    item_data['qty_in_carts'],
                    f'=SUM(C{{row}}:D{{row}})',
                    is_market_item(catalog_item),
                ])

        # Clear existing data (except headers) and write new data
        if data:
            worksheet.batch_clear(['A3:I1000'])  # Start clearing from row 3

            # Update the formulas with correct row numbers and ensure they're treated as formulas
            for i, row in enumerate(data, start=3):  # Start enumeration from row 3
                row[3] = f'=DGET(DesignerLookup,"Room Number",{{"Designer";C{i}}})'
                row[7] = f'=SUM(F{i}:G{i})'

            # Use raw parameter to ensure formulas are not escaped
            worksheet.update(values=data, range_name='A3', raw=False)  # Start data from row 3
            print(f"Successfully wrote {len(data)} items to the Pre-Orders sheet")
        else:
            print("No items to write to the sheet")

    except Exception as e:
        print(f"Error saving to Google Sheets: {e}")
        return False

    return True

def load_catalog_cache():
    """Load the catalog cache from disk if it exists"""
    try:
        if os.path.exists(CACHE_FILE_PATH):
            with open(CACHE_FILE_PATH, 'r') as f:
                return json.load(f)
    except Exception as e:
        print(f"Warning: Failed to load catalog cache: {e}")
    return {}

def save_catalog_cache():
    """Save the catalog cache to disk"""
    try:
        # Ensure the directory exists
        os.makedirs(os.path.dirname(CACHE_FILE_PATH), exist_ok=True)
        with open(CACHE_FILE_PATH, 'w') as f:
            json.dump(CATALOG_ITEMS_CACHE, f)
        print(f"Saved {len(CATALOG_ITEMS_CACHE)} items to catalog cache")
    except Exception as e:
        print(f"Warning: Failed to save catalog cache: {e}")

def main():
    global CATALOG_ITEMS_CACHE
    CATALOG_ITEMS_CACHE = load_catalog_cache()
    # Register the save function to run on exit
    atexit.register(save_catalog_cache)

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
            print(f"Cancelled: {ORDER_STATUS_COUNTS['CANCELED']}")
            print(f"\nSuccessfully processed {successful_orders} out of {len(orders)} orders")

            print("\nItem Quantities:")
            for item_id, item_data in ITEM_QUANTITIES.items():
                print(f"{item_data['name']} (ID: {item_id}): {item_data['quantity']} in carts: {item_data['qty_in_carts']}")

            # Save data to Google Sheets
            save_preorder_data(sheets_client)
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
