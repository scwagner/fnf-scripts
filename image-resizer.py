from PIL import Image
import os
import sys

def resize_image(image_path, max_dimension=1000):
    """
    Resize image if either dimension exceeds max_dimension while maintaining aspect ratio
    Returns the resized image if resizing was needed, None otherwise
    """
    with Image.open(image_path) as img:
        # Get original dimensions
        width, height = img.size

        # Check if resizing is needed
        if width <= max_dimension and height <= max_dimension:
            return None

        # Calculate new dimensions
        if width > height:
            new_width = max_dimension
            new_height = int((height * max_dimension) / width)
        else:
            new_height = max_dimension
            new_width = int((width * max_dimension) / height)

        # Resize and return
        return img.resize((new_width, new_height), Image.Resampling.LANCZOS)

def process_directory(directory_path):
    """
    Process all image files in the specified directory
    """
    # Create processed directory if it doesn't exist
    processed_dir = os.path.join(directory_path, "processed")
    if not os.path.exists(processed_dir):
        os.makedirs(processed_dir)

    # Supported image extensions
    image_extensions = ('.jpg', '.jpeg', '.png', '.gif', '.bmp')

    # Process each file in the directory
    for filename in os.listdir(directory_path):
        if filename.lower().endswith(image_extensions):
            file_path = os.path.join(directory_path, filename)

            try:
                # Attempt to resize image
                resized_image = resize_image(file_path)

                if resized_image:
                    # Move original file to processed directory
                    processed_path = os.path.join(processed_dir, filename)
                    os.rename(file_path, processed_path)

                    # Save resized image to original location
                    resized_image.save(file_path, quality=95)
                    print(f"Processed: {filename}")
                else:
                    print(f"Skipped: {filename} (already within size limits)")

            except Exception as e:
                print(f"Error processing {filename}: {str(e)}")

def main():
    if len(sys.argv) != 2:
        print("Usage: python image_resizer.py <directory_path>")
        sys.exit(1)

    directory_path = sys.argv[1]

    if not os.path.isdir(directory_path):
        print(f"Error: {directory_path} is not a valid directory")
        sys.exit(1)

    process_directory(directory_path)

if __name__ == "__main__":
    main()