import sys
import os
import asyncio
import aiofiles
import zipfile
import rarfile
import shutil
from tqdm import tqdm

# Add the ./scan_for_metadata/ directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'scan_for_metadata'))

# Now you can import the necessary classes
from diffusion_scanner import MetadataScanner, ImageGenerationMetadata, set_verbose_output

def debug(message):
    """Prints debug messages to the console."""
    print(f"[DEBUG] {message}")

async def process_image(file_path, dest_dir, scanner):
    """
    Processes an individual image file to check for IGMD (Image Generation Metadata) and save it to the destination directory.
    """
    debug(f"Processing image: {file_path}")
    file_params = ImageGenerationMetadata.read_metadata(file_path)
    if ImageGenerationMetadata.contains_image_generation_metadata(file_path):
        file_name = os.path.basename(file_path)
        dest_path = os.path.join(dest_dir, file_name)
        debug(f"IGMD found. Saving to: {dest_path}")
        async with aiofiles.open(dest_path, 'wb') as out_f:
            async with aiofiles.open(file_path, 'rb') as f:
                content = await f.read()
                await out_f.write(content)
        return True
    debug(f"No IGMD found in: {file_path}")
    return False

async def extract_image_from_archive(archive, name):
    """
    Asynchronously extracts an image from the archive (CBZ or CBR).
    """
    return await asyncio.to_thread(archive.read, name)

async def process_archive(file_path, dest_dir, scanner):
    """
    Processes a CBZ or CBR file asynchronously, extracts images, checks for IGMD, and saves the relevant files to the destination directory.
    If the file name contains 'merged' or 'chapter', it creates a folder with a numbered suffix.
    """
    debug(f"Processing archive: {file_path}")
    igmd_found = False
    file_name = os.path.splitext(os.path.basename(file_path))[0]

    # Detect file type and open the archive accordingly
    async with aiofiles.open(file_path, 'rb') as archive_file:
        archive_data = await archive_file.read()
    
    if file_path.lower().endswith('.cbz'):
        archive = zipfile.ZipFile(BytesIO(archive_data))
    elif file_path.lower().endswith('.cbr'):
        archive = rarfile.RarFile(BytesIO(archive_data))
    else:
        raise ValueError(f"Unsupported archive type: {file_path}")

    # Determine if the folder name should be modified for merged or chapter archives
    if "merged" in file_name.lower() or "chapter" in file_name.lower():
        count = 1
        sub_dir_base = os.path.join(dest_dir, f"{file_name}")
        sub_dir = f"{sub_dir_base}-{count}"
        while os.path.exists(sub_dir):
            count += 1
            sub_dir = f"{sub_dir_base}-{count}"
    else:
        sub_dir = os.path.join(dest_dir, file_name)

    os.makedirs(sub_dir, exist_ok=True)

    # Setup the progress bar
    file_names = [name for name in archive.namelist() if name.lower().endswith(('.jpg', '.jpeg', '.png', '.webp'))]
    progress_bar = tqdm(total=len(file_names), desc=f"Processing {file_name}", unit="file")

    tasks = []
    for name in file_names:
        tasks.append(process_image_in_archive(archive, name, file_path, sub_dir, progress_bar))

    results = await asyncio.gather(*tasks)

    if any(results):
        igmd_found = True
    
    if not igmd_found:
        debug(f"No IGMD found in archive: {file_path}")

    progress_bar.close()  # Ensure the progress bar is closed after processing

    return igmd_found

async def process_image_in_archive(archive, name, file_path, sub_dir, progress_bar):
    """
    Processes an individual image inside the archive (CBZ or CBR).
    """
    content = await extract_image_from_archive(archive, name)
    temp_file_path = os.path.join(sub_dir, "temp_image")

    try:
        async with aiofiles.open(temp_file_path, 'wb') as temp_file:
            await temp_file.write(content)
        
        if ImageGenerationMetadata.contains_image_generation_metadata(temp_file_path):
            out_name = f"{os.path.basename(sub_dir)}-{os.path.basename(name)}"
            dest_path = os.path.join(sub_dir, out_name)
            async with aiofiles.open(dest_path, 'wb') as out_f:
                await out_f.write(content)
            return True
    except Exception as e:
        debug(f"Error processing image {name} in archive: {e}")
    finally:
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        progress_bar.update(1)  # Update the progress bar after each file

    return False

async def process_files(scan_dir, dest_dir, log_non_igmd=None):
    """
    Scans and processes files from the scan directory, checks for IGMD, and saves relevant files to the destination directory.
    """
    debug(f"Starting scan in directory: {scan_dir}")
    scanner = MetadataScanner()
    tasks = []

    for root, dirs, files in os.walk(scan_dir):
        debug(f"Scanning directory: {root}")
        for file in files:
            file_path = os.path.join(root, file)
            ext = file.lower().split('.')[-1]

            if ext in ['jpg', 'jpeg', 'png', 'webp']:
                tasks.append(process_image(file_path, dest_dir, scanner))
            elif ext in ['cbz', 'cbr']:
                tasks.append(process_archive(file_path, dest_dir, scanner))

            if log_non_igmd:
                tasks.append(log_non_igmd_files(file_path, scanner, log_non_igmd, dest_dir, scan_dir))

    await asyncio.gather(*tasks)
    debug("Completed scanning and processing files.")

async def log_non_igmd_files(file_path, scanner, log_file, dest_dir, scan_dir):
    """
    Logs metadata from files that do not contain IGMD if the log_non_igmd option is specified.
    """
    debug(f"Checking for IGMD in file: {file_path}")
    if not ImageGenerationMetadata.contains_image_generation_metadata(file_path):
        exiftool_path = "exiftool"  # Ensure exiftool is installed on the system
        command = [
            exiftool_path,
            "-FileName",
            "-Parameters",
            "-Prompt",
            "-UserComment",
            "-Model",
            "-Lora",
            "-cfg scale",
            "-seed",
            "-NegativePrompt",
            "-ModelHash",
            file_path
        ]
        process = await asyncio.create_subprocess_exec(
            *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        if process.returncode == 0:
            debug(f"Logging non-IGMD metadata for file: {file_path}")
            async with aiofiles.open(log_file, 'a') as log_f:
                await log_f.write(stdout.decode())
                await log_f.write('\n')
        else:
            debug(f"ExifTool error on file {file_path}: {stderr.decode()}")
            print(f"ExifTool error: {stderr.decode()}", file=sys.stderr)

def check_requirements():
    """
    Checks if all required Python modules and binaries are installed.
    """
    debug("Checking required modules and binaries.")
    try:
        import aiofiles
        import zipfile
        import rarfile
    except ImportError as e:
        debug(f"Missing required Python module: {e.name}")
        print(f"Missing required Python module: {e.name}", file=sys.stderr)
        sys.exit(1)

    if shutil.which("exiftool") is None:
        debug("ExifTool is not installed or not found in system PATH.")
        print("ExifTool is not installed or not found in system PATH.", file=sys.stderr)
        sys.exit(1)

def validate_directories(scan_dir, dest_dir, log_file):
    """
    Validates the scan directory, destination directory, and log file paths.
    """
    debug("Validating directories.")
    if not os.path.exists(scan_dir):
        debug(f"Scan directory '{scan_dir}' does not exist.")
        print(f"Scan directory '{scan_dir}' does not exist.", file=sys.stderr)
        sys.exit(1)
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
        debug(f"Created destination directory: {dest_dir}")

    if os.path.commonpath([scan_dir]) == os.path.commonpath([dest_dir]):
        debug("Destination directory cannot be inside the scan directory.")
        print("Destination directory cannot be inside the scan directory.", file=sys.stderr)
        sys.exit(1)

    if log_file:
        if os.path.commonpath([log_file, scan_dir]) == scan_dir or os.path.commonpath([log_file, dest_dir]) == dest_dir:
            debug("Log file cannot be inside the scan or destination directories.")
            print("Log file cannot be inside the scan or destination directories.", file=sys.stderr)
            sys.exit(1)

def print_help():
    """
    Prints help documentation for the script usage.
    """
    help_text = """
    Usage: find_extract_IGMD.py --scan_dir <scan_directory> --dest_dir <destination_directory> [--log_non-igmd <log_file>]

    Arguments:
    --scan_dir           Directory to scan for images, CBZ, and CBR files.
    --dest_dir           Directory to save extracted IGMD files.
    --log_non-igmd       (Optional) Log file to store metadata of non-IGMD files.
    --help               Display this help message.
    """
    print(help_text)

if __name__ == '__main__':
    if '--help' in sys.argv or len(sys.argv) < 5:
        print_help()
        sys.exit(0)

    try:
        scan_dir = sys.argv[sys.argv.index('--scan_dir') + 1]
        dest_dir = sys.argv[sys.argv.index('--dest_dir') + 1]
    except (ValueError, IndexError):
        print_help()
        sys.exit(1)

    log_non_igmd = None
    if '--log_non-igmd' in sys.argv:
        try:
            log_non_igmd = sys.argv[sys.argv.index('--log_non-igmd') + 1]
        except (ValueError, IndexError):
            print_help()
            sys.exit(1)

    check_requirements()
    validate_directories(scan_dir, dest_dir, log_non_igmd)

    debug("Starting the file processing.")
    asyncio.run(process_files(scan_dir, dest_dir, log_non_igmd))
    debug("Script execution completed.")
