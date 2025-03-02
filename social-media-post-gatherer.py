#!/usr/bin/env python3
import os
import requests
from dotenv import load_dotenv
import shutil

load_dotenv()
api_key = os.getenv('SQUARE_API_KEY')
master_parent_category = 'SDHEFXDOD4ADOMZ7EDP47PRB'
api_root = 'https://connect.squareup.com/v2'
image_dir = os.getenv('SQUARE_IMAGE_DIR')


def get_headers():
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Square-Version": "2025-01-23",
    }


def is_child_category(category):
    item_type = category.get('type', '')
    if item_type != 'CATEGORY':
        return False

    category_data = category.get("category_data", {})
    parent_category = category_data.get("parent_category", {})
    parent_category_id = parent_category.get("id", "")
    return parent_category_id == master_parent_category


def get_child_categories():
    url = f"{api_root}/catalog/search"
    params = {
        "object_types": ["CATEGORY"],
    }
    response = requests.post(url, headers=get_headers(), json=params)
    response_json = response.json()
    return [x for x in response_json["objects"] if is_child_category(x)]


def get_image_url(image_id):
    url = f"{api_root}/catalog/object/{image_id}"
    response = requests.get(url, headers=get_headers())
    response_json = response.json()
    image_url = response_json["object"]["image_data"]["url"]
    return image_url


def process_child_category(child_category):
    # Create directory for child category if it doesn't exist
    category_dir = child_category["category_data"]["name"]
    os.makedirs(os.path.join(image_dir, category_dir), exist_ok=True)

    # Write shop URL to file
    shop_url = f"https://www.flossandflame.com/shop/{child_category['id']}"
    with open(os.path.join(image_dir, category_dir, "shop.txt"), "w") as f:
        f.write(f"{shop_url}\n\nNashville Needlework Market new releases from {child_category['category_data']['name']} are available for pre-order at our website at {shop_url}. All cross stitch orders ship FREE!\n\n")

    print(f'{child_category["category_data"]["name"]} {child_category["id"]}')
    params = {
        "category_ids": [child_category["id"]],
    }
    url = f"{api_root}/catalog/search-catalog-items"
    response = requests.post(url, headers=get_headers(), json=params)
    response_json = response.json()
    if not "items" in response_json:
        print(f"No items found for {child_category['category_data']['name']}")
        return
    items = response_json["items"]
    for item in items:
        print(item["item_data"]["name"])
        name = item["item_data"]["name"]
        if ":" in name and "by" in name:
            name = name.split(":")[1].split("by")[0].strip()
            print(f"Cleaned name: {name}")
        if ":" in name and " - " in name:
            name = name.split(":")[1].split(" - ")[0].strip()
            print(f"Cleaned name: {name}")
        with open(os.path.join(image_dir, category_dir, "shop.txt"), "a") as f:
            f.write(f"\n{name}")
        image_id = item["item_data"]["image_ids"][0]
        print(f"Image ID: {image_id}")
        cache_filename = os.path.join(image_dir, "cache", f"{image_id}")
        file_ext = ""
        if os.path.exists(cache_filename):
            # Get file extension from existing cached image
            cached_files = [f for f in os.listdir(cache_filename) if f.startswith('image.')]
            if cached_files:
                file_ext = os.path.splitext(cached_files[0])[1]
                print(f"Using cached image with extension {file_ext}")
        if len(file_ext) == 0:
            os.makedirs(cache_filename, exist_ok=True)
            image_url = get_image_url(image_id)
            print(f"Image URL: {image_url}")
            if len(image_url) > 0:
                # Get file extension from URL
                file_ext = os.path.splitext(image_url)[1]

                # Download and save image
                img_response = requests.get(image_url)
                if img_response.status_code == 200:
                    with open(os.path.join(cache_filename, f"image{file_ext}"), "wb") as f:
                        f.write(img_response.content)
                    print(f"Saved image to {cache_filename}")
                else:
                    print(f"Failed to download image from {image_url}")
        if len(file_ext) == 0:
            raise Exception(f"Could not determine file extension for image {image_id}")
        product_filename = os.path.join(image_dir, category_dir, f"{name}{file_ext}")
        if not os.path.exists(product_filename):
            shutil.copy2(os.path.join(cache_filename, f"image{file_ext}"), product_filename)
            print(f"Copied {os.path.join(cache_filename, f'image{file_ext}')} to {product_filename}")



def main():
    if not api_key or not len(api_key):
        print("No API key found")
        return
    child_categories = get_child_categories()
    for child_category in child_categories:
        process_child_category(child_category)


if __name__ == "__main__":
    main()


