import sys
import subprocess
from diffusion_scanner import MetadataScanner, ImageGenerationMetadata

def scan_directory_for_metadata(directory: str):
    # Get all image files in the directory
    image_files = MetadataScanner.get_files(directory, extensions="png,jpeg,jpg,webp")

    if not image_files:
        print(f"No image files found in directory: {directory}")
        return

    # Scan each image file for metadata and run exiftool if no metadata is found
    for image_file in image_files:
        has_metadata = ImageGenerationMetadata.contains_image_generation_metadata(image_file)
        
        if not has_metadata:
            print(f"MetaData NOTFOUND in {image_file}")
            run_exiftool(image_file)

def run_exiftool(image_file):
    try:
        result = subprocess.run(['exiftool', image_file], capture_output=True, text=True)
        print(result.stdout)
    except FileNotFoundError:
        print("ExifTool is not installed or not found in the system PATH.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python test_diffusion_scan_directory.py <directory_path>")
        sys.exit(1)

    directory_path = sys.argv[1]
    scan_directory_for_metadata(directory_path)
