#!/usr/bin/env python3

import os
import sys
from pathlib import Path
import subprocess
import shutil

def convert_to_png(directory):
    # Convert directory to Path object for easier handling
    dir_path = Path(directory)
    
    # Check if directory exists
    if not dir_path.is_dir():
        print(f"Error: Directory '{directory}' does not exist")
        sys.exit(1)
    
    # Create 'converted' subdirectory if it doesn't exist
    converted_dir = dir_path / 'converted'
    converted_dir.mkdir(exist_ok=True)
    
    # Supported input formats
    supported_formats = {'.avif', '.webp'}
    
    # Enumerate all files in the directory
    for file_path in dir_path.iterdir():
        # Skip the converted directory itself
        if file_path == converted_dir:
            continue
            
        # Check if file is in a supported format
        if file_path.suffix.lower() in supported_formats:
            # Construct the PNG filename in the original directory
            png_path = file_path.with_suffix('.png')
            # Construct path for the original file in converted directory
            converted_path = converted_dir / file_path.name
            
            try:
                if png_path.exists():
                    # If PNG exists, just move the original file
                    shutil.move(str(file_path), str(converted_path))
                    print(f"PNG exists, moved original {file_path.name} to converted/")
                else:
                    # Convert to PNG and move original if successful
                    result = subprocess.run(
                        ['convert', str(file_path), str(png_path)],
                        capture_output=True,
                        text=True
                    )
                    
                    if result.returncode == 0:
                        # Move original file to converted directory
                        shutil.move(str(file_path), str(converted_path))
                        print(f"Converted: {file_path.name} -> {png_path.name}")
                        print(f"Moved original to: {converted_path}")
                    else:
                        print(f"Error converting {file_path.name}: {result.stderr}")
            except Exception as e:
                print(f"Error processing {file_path.name}: {str(e)}")

def main():
    # Check if directory argument is provided
    if len(sys.argv) != 2:
        print("Usage: python convert-image.py <directory>")
        sys.exit(1)
    
    directory = sys.argv[1]
    convert_to_png(directory)

if __name__ == "__main__":
    main() 