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
import time

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

# Add this near the top with other globals
DESIGNER_ROOM_NUMBERS = {}  # Will store {designer_name: room_number}

# Add near the top with other globals
CUSTOMER_ORDERS = {}  # Will store {customer_name: [list of order details]}
SECOND_SHEET_URL = "https://docs.google.com/spreadsheets/d/1hhXCfphftezK_W1NYXeyFtYJgY8g9taITaQ0yUiTBx0/edit?gid=0#gid=0"

# Add near the top with other globals
RATE_LIMIT_SLEEP = 1  # Sleep duration in seconds between API calls

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
            return False
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
        customer_name = customer_info['name']

        # Skip if no customer name
        if customer_name == 'N/A':
            return False

        # Initialize customer's order list if not exists
        if customer_name not in CUSTOMER_ORDERS:
            CUSTOMER_ORDERS[customer_name] = []

        # Collect order details
        order_details = {
            'order_id': order.get_order_id(),
            'created_at': order.get_created_at(),
            'status': order.get_fulfillment_status(),
            'items': []
        }

        # Process line items
        for item in order.get_line_items():
            order_item = OrderItem(item)
            catalog_object_id = item.get('catalog_object_id')
            quantity = int(float(item.get('quantity', 0)))

            # Add item details to order
            order_details['items'].append({
                'name': order_item.get_name(),
                'quantity': quantity
            })

            # Update global item quantities as before
            if catalog_object_id:
                if catalog_object_id not in ITEM_QUANTITIES:
                    ITEM_QUANTITIES[catalog_object_id] = {
                        'name': order_item.get_name(),
                        'quantity': quantity,
                        'qty_in_carts': 0,
                    }
                else:
                    ITEM_QUANTITIES[catalog_object_id]['quantity'] += quantity

        # Add order details to customer's orders
        CUSTOMER_ORDERS[customer_name].append(order_details)

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

def load_designer_room_numbers(sheets_client):
    """Load designer room numbers from the Designers sheet"""
    try:
        sheet_url = os.getenv('GOOGLE_SHEET_URL')
        spreadsheet = sheets_client.open_by_url(sheet_url)

        try:
            designers_sheet = spreadsheet.worksheet('Designers')
            # Get all values from the sheet
            all_values = designers_sheet.get_all_values()

            # Skip header row and create dictionary
            for row in all_values[1:]:
                if len(row) >= 2 and row[0] and row[1]:  # If designer and room number exist
                    DESIGNER_ROOM_NUMBERS[row[0]] = row[1]

            print(f"Loaded {len(DESIGNER_ROOM_NUMBERS)} designer room numbers")
        except gspread.WorksheetNotFound:
            print("Warning: Designers worksheet not found")

    except Exception as e:
        print(f"Error loading designer room numbers: {e}")

def rate_limited_update(worksheet, values, range_name=None, value_input_option='RAW'):
    """
    Perform a rate-limited update to Google Sheets
    Args:
        worksheet: The worksheet to update
        values: The values to write
        range_name: Optional range name for the update
        value_input_option: The input option for the update
    """
    try:
        if range_name:
            worksheet.update(values=values, range_name=range_name, value_input_option=value_input_option)
        else:
            worksheet.update(values=values, value_input_option=value_input_option)
        print(f"Sleeping for {RATE_LIMIT_SLEEP}s to respect API quota...")
        time.sleep(RATE_LIMIT_SLEEP)
    except Exception as e:
        print(f"Error during worksheet update: {e}")
        raise

def save_preorder_data(sheets_client):
    """
    Save preorder data to Google Sheets in the 'Pre-Orders' worksheet
    Args:
        sheets_client: Authorized Google Sheets client
    """
    try:
        # Load designer room numbers first
        load_designer_room_numbers(sheets_client)

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
            f"Order Summary: {ORDER_STATUS_COUNTS['NOT_FULFILLED']} orders processed | Sheet last updated: {current_time}",
            "", "", ""  # Reduced from 6 empty strings to 4 total columns
        ]]

        # Set up headers - now in row 2
        headers = [['Designer', 'Room Number', 'Item', 'Pre-ordered']]

        # Update summary and headers with rate limiting
        rate_limited_update(worksheet, summary + headers, 'A1:D2')

        # Format rows with rate limiting
        worksheet.format('A1:D1', {
            "textFormat": {
                "bold": True,
                "fontSize": 12
            }
        })
        time.sleep(RATE_LIMIT_SLEEP)
        worksheet.format('A2:D2', {
            "backgroundColor": {
                "red": 0.8,
                "green": 0.9,
                "blue": 1.0
            },
            "textFormat": {
                "bold": True
            }
        })
        time.sleep(RATE_LIMIT_SLEEP)

        # Modify the data preparation section
        data = []
        for item_id, item_data in ITEM_QUANTITIES.items():
            catalog_item = get_catalog_item(item_id)
            if catalog_item and is_market_item(catalog_item):
                designer = extract_designer_name(item_data['name'])
                room_number = DESIGNER_ROOM_NUMBERS.get(designer, '')  # Get room number or empty string

                # Convert room number to int for sorting, using -1 for empty/invalid numbers
                try:
                    room_num_sort = int(room_number) if room_number else -1
                except ValueError:
                    room_num_sort = -1

                data.append([
                    designer,
                    room_number,
                    extract_item_name(item_data['name']),
                    item_data['quantity'],
                    room_num_sort  # Add sort key but don't write to sheet
                ])

        # Sort data by room number (descending) and then by item name
        data.sort(key=lambda x: (-x[4], x[2]))  # Updated indices since we removed two columns

        # Add blank rows between different room numbers
        final_data = []
        prev_room = None
        for row in data:
            current_room = row[1]  # Room number is in index 1
            if prev_room is not None and current_room != prev_room:
                final_data.append(['', '', '', ''])  # Add blank row
            final_data.append(row[:-1])  # Add row without sort key
            prev_room = current_room

        # Clear and update data with rate limiting
        if final_data:
            worksheet.batch_clear(['A3:D1000'])
            time.sleep(RATE_LIMIT_SLEEP)
            rate_limited_update(worksheet, final_data, 'A3')
            print(f"Successfully wrote {len(data)} items to the Pre-Orders sheet")
        else:
            print("No items to write to the sheet")

        # After writing data to Pre-Orders sheet, collect all designers
        designers = set()  # Use a set to avoid duplicates
        if data:
            for row in data:
                designer = row[0]  # Designer is in column A (index 0)
                if designer and designer != 'Unknown Designer':
                    designers.add(designer)

        # Now handle the Designers sheet
        try:
            designers_sheet = spreadsheet.worksheet('Designers')
        except gspread.WorksheetNotFound:
            designers_sheet = spreadsheet.add_worksheet('Designers', 1000, 3)
            time.sleep(RATE_LIMIT_SLEEP)
            rate_limited_update(designers_sheet, [['Designer', 'Room Number', 'Notes']], 'A1:C1')

        # Get existing designers
        existing_designers = designers_sheet.col_values(1)[1:]  # Skip header row

        # Find new designers to add
        new_designers = designers - set(existing_designers)

        if new_designers:
            # Get the next empty row
            next_row = len(existing_designers) + 2  # +2 for header row and 1-based index

            # Prepare new rows
            new_rows = [[designer, '', ''] for designer in new_designers]

            # Add new designers
            rate_limited_update(designers_sheet, new_rows, f'A{next_row}:C{next_row + len(new_rows) - 1}')
            print(f"Added {len(new_rows)} new designers to Designers sheet")
        else:
            print("No new designers to add")

    except Exception as e:
        print(f"Error saving to Google Sheets: {e}")
        return False

    return True

def format_customer_worksheet(worksheet, customer_name):
    """Format the customer worksheet with header and styling"""
    # Add customer name as header
    worksheet.merge_cells('A1:D1')  # Reduced to 4 columns
    rate_limited_update(worksheet, [[customer_name]], 'A1:D1')

    # Format customer name
    worksheet.format('A1:D1', {  # Reduced to 4 columns
        "textFormat": {
            "bold": True,
            "fontSize": 14
        },
        "horizontalAlignment": "center",
        "backgroundColor": {"red": 0.9, "green": 0.9, "blue": 0.9}
    })
    time.sleep(RATE_LIMIT_SLEEP)

    # Add headers in row 3
    headers = [['Pre-Order', 'Designer', 'Item Name', 'Quantity']]  # Removed Order ID, Created At, Status
    rate_limited_update(worksheet, headers, 'A3:D3')  # Reduced to 4 columns

    # Format headers
    worksheet.format('A3:D3', {  # Reduced to 4 columns
        "backgroundColor": {"red": 0.8, "green": 0.9, "blue": 1.0},
        "textFormat": {"bold": True}
    })
    time.sleep(RATE_LIMIT_SLEEP)

def process_item_details(item_name):
    """Process item name to extract pre-order status, designer, and clean name"""
    is_preorder = item_name.startswith('PRE-ORDER: ')

    if is_preorder:
        clean_name = extract_item_name(item_name)
        designer = extract_designer_name(item_name)
        preorder_status = ''
    else:
        clean_name = item_name
        designer = ''
        preorder_status = 'No'

    return preorder_status, designer, clean_name

def save_customer_orders(sheets_client):
    try:
        spreadsheet = sheets_client.open_by_url(SECOND_SHEET_URL)

        # Rename Sheet1 to !Summary if needed
        try:
            summary_sheet = spreadsheet.sheet1
            if summary_sheet.title != '!Summary':
                summary_sheet.update_title('!Summary')
                time.sleep(RATE_LIMIT_SLEEP)
        except Exception as e:
            print(f"Error renaming summary sheet: {e}")

        # Dictionary to store sheet IDs
        sheet_ids = {}

        summary_data = []
        summary_headers = ['Customer Name', 'Order Count', 'Link to Details']

        for customer_name, orders in sorted(CUSTOMER_ORDERS.items()):  # Sort customers alphabetically
            safe_name = ''.join(c for c in customer_name if c.isalnum() or c.isspace())[:31]

            try:
                try:
                    worksheet = spreadsheet.worksheet(safe_name)
                    worksheet.clear()
                    time.sleep(RATE_LIMIT_SLEEP)
                except gspread.WorksheetNotFound:
                    worksheet = spreadsheet.add_worksheet(safe_name, 1000, 4)  # Reduced columns to 4
                    time.sleep(RATE_LIMIT_SLEEP)

                sheet_ids[safe_name] = worksheet.id

                # Add summary data
                sheet_link = f'=HYPERLINK("#gid={sheet_ids[safe_name]}", "View Details")'
                summary_data.append([
                    customer_name,
                    len(orders),
                    sheet_link
                ])

                # Format worksheet with customer header
                format_customer_worksheet(worksheet, customer_name)

                # Prepare and update data
                data = []
                for order in orders:
                    for item in order['items']:
                        preorder_status, designer, clean_name = process_item_details(item['name'])
                        data.append([
                            preorder_status,
                            designer,
                            clean_name,
                            item['quantity']
                        ])

                if data:
                    rate_limited_update(worksheet, data, f'A4:D{len(data)+3}')

                print(f"Updated worksheet for customer: {customer_name}")

            except Exception as e:
                print(f"Error processing worksheet for {customer_name}: {e}")
                continue

        # Reorder sheets alphabetically
        try:
            worksheets = spreadsheet.worksheets()
            # Sort worksheets, keeping !Summary first
            sorted_worksheets = sorted(worksheets, key=lambda x: (x.title != '!Summary', x.title.lower()))

            # Reorder sheets
            for i, worksheet in enumerate(sorted_worksheets):
                if worksheet.index != i:
                    spreadsheet.reorder_worksheets([worksheet], i)
                    time.sleep(RATE_LIMIT_SLEEP)
        except Exception as e:
            print(f"Error reordering sheets: {e}")

        # Update !Summary sheet
        try:
            summary_sheet = spreadsheet.worksheet('!Summary')
            summary_sheet.clear()
            time.sleep(RATE_LIMIT_SLEEP)

            # Update headers
            rate_limited_update(summary_sheet, [summary_headers], 'A1:C1')

            # Format headers
            summary_sheet.format('A1:C1', {
                "backgroundColor": {"red": 0.8, "green": 0.9, "blue": 1.0},
                "textFormat": {"bold": True}
            })
            time.sleep(RATE_LIMIT_SLEEP)

            # Update data
            if summary_data:
                rate_limited_update(summary_sheet, summary_data, f'A2:C{len(summary_data)+1}', value_input_option='USER_ENTERED')

            print(f"Updated summary sheet with {len(summary_data)} customer entries")

        except Exception as e:
            print(f"Error updating summary sheet: {e}")

    except Exception as e:
        print(f"Error saving customer orders to second spreadsheet: {e}")
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

            # Save data to both spreadsheets
            save_preorder_data(sheets_client)
            save_customer_orders(sheets_client)
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
