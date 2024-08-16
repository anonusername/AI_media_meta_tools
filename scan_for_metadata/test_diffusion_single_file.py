import sys
import subprocess
from diffusion_scanner import ImageGenerationMetadata

def test_image_metadata(image_file_path):
    has_metadata = ImageGenerationMetadata.contains_image_generation_metadata(image_file_path)
    
    if not has_metadata:
        print(f"MetaData NOTFOUND in {image_file_path}")
        run_exiftool(image_file_path)

def run_exiftool(image_file):
    try:
        result = subprocess.run(['exiftool', image_file], capture_output=True, text=True)
        print(result.stdout)
    except FileNotFoundError:
        print("ExifTool is not installed or not found in the system PATH.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python test_diffusion_single_file.py <image_file_path>")
        sys.exit(1)

    image_file_path = sys.argv[1]
    test_image_metadata(image_file_path)
