import csv
import json
import re
import random
import time
import uuid
import requests
from square import Square
from square.environment import SquareEnvironment

square = Square(
    environment=SquareEnvironment.PRODUCTION,
    token="EAAAlnecrJ2nvfBUNgudpxKkKdx9-kC9SSZA7-FixVN--6t3UUe8VUPHwM--lzYs"
)

headers = {
    "Authorization": "Bearer EAAAlnecrJ2nvfBUNgudpxKkKdx9-kC9SSZA7-FixVN--6t3UUe8VUPHwM--lzYs",
    "Accept": "application/json",
    "Square-Version": "2025-04-16",
}

def read_json_template(template_file):
    """Read the JSON template file"""
    with open(template_file, 'r') as file:
        return file.read()

def substitute_variables(template_str, variables):
    """Substitute variables in the template with values from the dictionary"""
    # Handle special variables
    if 'currentTimestamp' not in variables:
        variables['currentTimestamp'] = int(time.time() * 1000)

    if 'idempotencyKey' not in variables:
        variables['idempotencyKey'] = str(uuid.uuid4())

    if 'randomNumber' not in variables:
        variables['randomNumber'] = random.randint(-9999999999999999, 9999999999999999)

    # Regular expression to find ${variable} patterns
    pattern = r'\$\{([^}]+)\}'

    def replace_var(match):
        var_name = match.group(1)
        if var_name in variables:
            value = variables[var_name]
            # Handle numeric values without quotes
            if isinstance(value, (int, float, str)):
                return str(value)
            return f'"{value}"'
        return match.group(0)  # Return unchanged if variable not found

    # Replace all variables in the template
    return re.sub(pattern, replace_var, template_str)

def upload_image(object_id, image_slug, local_filename, item_name):
    print(f"Uploading image {local_filename} to {object_id} with slug {image_slug}")

    request={
        "idempotency_key": str(uuid.uuid4()),
        "is_primary": True,
        "object_id": object_id,
        "image": {
            "id": f"#{image_slug}",
            "type": "IMAGE",
            "image_data": {
                "caption": item_name,
                "name": image_slug,
            }
        }
    }

    response = requests.post(
        "https://connect.squareup.com/v2/catalog/images",
        headers=headers,
        files={"image_file": (local_filename, open(local_filename, 'rb'), 'image/jpeg'), "request": json.dumps(request)}
    )

    print(response.json())
    return

def process_row(row):
    """Process a single row from the CSV file"""
    # This is a placeholder - customize this function as needed
    print(f"Processing row: {row}")

    # Example: Create JSON from template using this row's data
    template = read_json_template("floss-template.json")
    processed_json = substitute_variables(template, row)

    # Here you can save the processed JSON or perform other operations
    # For example, save to a file named after a field in the row:
    if 'sku' in row:
        # output_filename = f"output_{row['sku']}.json"
        # with open(output_filename, 'w') as file:
        #     file.write(processed_json)
        # print(f"Created {output_filename}")

        response = requests.post(
            "https://connect.squareup.com/v2/catalog/object",
            headers=headers,
            json=json.loads(processed_json)
        )

        print(response.json())
        if response.status_code != 200:
            print(f"Error: {response.json()}")
            return None

        object_id = response.json()['catalog_object']['id']
        if 'existingPicture' in row and len(row['existingPicture']) > 0:
            upload_image(object_id, row['imageSlug'], f"Images/Photo/{row['existingPicture']}", row['flossName'])

        time.sleep(1)

    return True

def main():
    """Main function to read CSV and process each row"""
    try:
        number_processed = 0
        number_failed = 0
        with open('gloriana.csv', 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                result = process_row(row)
                if result is not None:
                    number_processed += 1
                else:
                    number_failed += 1
                    print(f"Failed to process row: {row}")

    except FileNotFoundError:
        print("Error: gloriana.csv file not found")
    except Exception as e:
        print(f"An error occurred: {e}")

    print(f"Processed {number_processed} rows, failed {number_failed} rows")

if __name__ == "__main__":
    main()