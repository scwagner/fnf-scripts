#!/usr/bin/env python3

import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import sys

def download_image(image_url, save_path, host_header=None, ip_address=None):
    """Download an image from a URL and save it to the specified path."""
    try:
        # Check if this is a URL or a local file path
        if image_url.startswith(('http://', 'https://')):
            # It's a URL - download it
            headers = {}
            if host_header:
                headers['Host'] = host_header

            # If an IP address is specified, replace the hostname in the URL
            if ip_address:
                parsed_url = urlparse(image_url)
                # Use the scheme and path from the original URL but with the IP address
                if parsed_url.port:
                    request_url = f"{parsed_url.scheme}://{ip_address}:{parsed_url.port}{parsed_url.path}"
                    if parsed_url.query:
                        request_url += f"?{parsed_url.query}"
                else:
                    request_url = f"{parsed_url.scheme}://{ip_address}{parsed_url.path}"
                    if parsed_url.query:
                        request_url += f"?{parsed_url.query}"
            else:
                request_url = image_url

            response = requests.get(request_url, headers=headers, stream=True)
            response.raise_for_status()  # Raise an exception for HTTP errors

            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
        else:
            # This is likely an error since we should be handling local files differently
            print(f"Cannot download local path: {image_url}")
            return False

        print(f"Downloaded: {save_path}")
        return True
    except Exception as e:
        print(f"Error downloading {image_url}: {e}")
        return False

def process_html(html_file, base_url, host_header=None, ip_address=None):
    """Process HTML file and download missing images."""
    # Get base directory of the HTML file for relative paths
    base_dir = os.path.dirname(os.path.abspath(html_file))

    # Parse HTML
    with open(html_file, 'r', encoding='utf-8', errors='ignore') as f:
        soup = BeautifulSoup(f.read(), 'html.parser')

    # Find all image tags
    img_tags = soup.find_all('img')
    print(f"Found {len(img_tags)} image tags in {html_file}")

    downloaded_count = 0
    skipped_count = 0
    error_count = 0

    for img in img_tags:
        # Get the image source
        src = img.get('src')
        if not src:
            continue

        # Create proper file path for saving
        img_path = os.path.join(base_dir, src)

        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(img_path), exist_ok=True)

        # Check if image already exists
        if os.path.exists(img_path):
            print(f"Skipping existing image: {img_path}")
            skipped_count += 1
            continue

        # Create full URL for downloading
        full_url = urljoin(base_url, src)

        # Download the image
        if download_image(full_url, img_path, host_header, ip_address):
            downloaded_count += 1
        else:
            error_count += 1

    print(f"\nSummary:\n  Downloaded: {downloaded_count}\n  Skipped: {skipped_count}\n  Errors: {error_count}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        html_file = sys.argv[1]
    else:
        html_file = "sample.html"  # Default to sample.html if no argument provided

    # Check if file exists
    if not os.path.exists(html_file):
        print(f"Error: File {html_file} not found")
        sys.exit(1)

    # Ask user for the Host header
    host_header = input("Enter the hostname for the Host header (leave empty for default): ")

    # Ask user for IP address
    ip_address = input("Enter the IP address of the web server (leave empty to use hostname): ")

    # Ask user for base URL
    base_url = input("Enter the base URL for downloading images (e.g., http://example.com/): ")
    if not base_url:
        base_url = "http://example.com/"

    # Make sure base URL ends with slash
    if not base_url.endswith('/'):
        base_url += '/'

    process_html(html_file, base_url, host_header, ip_address)