import subprocess
import sys
import os
import time
import zipfile
import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from aiomultiprocess import Pool
import logging
from tqdm import tqdm
import numpy as np

# Pre-check for required modules and exiftool
def pre_check():
    # List of required Python modules
    required_modules = ['tqdm', 'numpy', 'aiomultiprocess']

    # Check and install missing modules
    for module in required_modules:
        try:
            __import__(module)
        except ImportError:
            print(f"Module '{module}' not found. Installing...")
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', module])

    # Check for exiftool
    try:
        subprocess.run(['exiftool', '-ver'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        print("ExifTool is installed and available.")
    except subprocess.CalledProcessError:
        print("ExifTool is not installed or not found in the system PATH.")
        sys.exit(1)

# Run pre-checks before starting the main script
pre_check()

# Define the CBZ file path and output directory
cbz_file_path = 'scan_for_metadata/Test_CBZs/FavAI8.cbz'
output_dir = 'output_directory'

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# Utility function to check for keywords in image metadata
def check_metadata(image_file_path):
    try:
        result = subprocess.run(
            ['exiftool', image_file_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        metadata = result.stdout
        if any(word in metadata.lower() for word in ["model", "lora", "negative", "prompt"]):
            return True
    except Exception as e:
        logging.error(f"Failed to run ExifTool on {image_file_path}: {e}")
    return False

# Function moved to the global scope
def process_and_check_metadata(zip_file_path, file_info, output_dir, counts):
    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            with zip_ref.open(file_info.filename) as image_file:
                temp_path = os.path.join(output_dir, file_info.filename)
                with open(temp_path, 'wb') as temp_image:
                    temp_image.write(image_file.read())
                if check_metadata(temp_path):
                    logging.info(f"Keyword found in {file_info.filename}")
                    counts['found'] += 1
                else:
                    counts['not_found'] += 1
                os.remove(temp_path)
    except Exception as e:
        logging.error(f"Failed to process {file_info.filename}: {e}")
        counts['non_image'] += 1

# Async version of the function for aiomultiprocess
async def async_process_and_check_metadata(zip_file_path, file_info, output_dir, counts):
    return await asyncio.to_thread(process_and_check_metadata, zip_file_path, file_info, output_dir, counts)

# Sort files by size for better load balancing
def sort_files_by_size(zip_ref):
    return sorted(zip_ref.infolist(), key=lambda f: f.file_size, reverse=True)

# Sort log entries by filename
def sort_log_by_filename(log_file, cbz_file_path, processing_summary):
    try:
        with open(log_file, 'r') as f:
            lines = f.readlines()
        sorted_lines = sorted(lines, key=lambda line: line.split(" ")[-1].strip())
        with open(log_file, 'w') as f:
            f.write(f"CBZ File: {cbz_file_path}\n")
            f.write(f"Fastest items/sec: {processing_summary['max_speed']:.2f} | Slowest items/sec: {processing_summary['min_speed']:.2f} | Mean items/sec: {processing_summary['mean_speed']:.2f}\n")
            f.write(f"Keys found: {processing_summary['found']} | Keys not found: {processing_summary['not_found']} | Number of Non-Images: {processing_summary['non_image']} | Total files: {processing_summary['total']}\n")
            f.writelines(sorted_lines)
    except FileNotFoundError:
        logging.error(f"Log file {log_file} not found during sorting.")

# Function to record and log processing speeds
def log_processing_speeds(log_file, times, counts):
    if times:
        speeds = np.divide(1.0, np.array(times), where=(np.array(times) > 0))
        processing_summary = {
            'min_speed': speeds.min(),
            'max_speed': speeds.max(),
            'mean_speed': speeds.mean(),
            'found': counts['found'],
            'not_found': counts['not_found'],
            'non_image': counts['non_image'],
            'total': counts['found'] + counts['not_found'] + counts['non_image'],
        }

        with open(log_file, 'a') as f:
            f.write(f"\nFastest items/sec: {processing_summary['max_speed']:.2f}\n")
            f.write(f"Slowest items/sec: {processing_summary['min_speed']:.2f}\n")
            f.write(f"Mean items/sec: {processing_summary['mean_speed']:.2f}\n")
    else:
        logging.error("No processing times recorded.")
        processing_summary = {'min_speed': 0, 'max_speed': 0, 'mean_speed': 0, 'found': 0, 'not_found': 0, 'non_image': 0, 'total': 0}
    
    return processing_summary

# Initialize logging configuration for each method
def initialize_logging(log_file):
    # Clear any existing handlers
    logging.getLogger().handlers.clear()
    logging.basicConfig(filename=log_file, level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

# Method 1: Thread Pool
def thread_pool_method(zip_file_path, output_dir):
    log_file = 'metadata_extraction_method_1_thread_pool.log'
    initialize_logging(log_file)

    start_time = time.time()
    processing_times = []
    counts = {'found': 0, 'not_found': 0, 'non_image': 0}

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        file_infos = sort_files_by_size(zip_ref)
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = []
            with tqdm(total=len(file_infos), desc="Thread Pool Method", unit="file") as pbar:
                for file_info in file_infos:
                    if file_info.filename.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp', '.gif')):
                        start = time.time()
                        future = executor.submit(process_and_check_metadata, zip_file_path, file_info, output_dir, counts)
                        future.add_done_callback(lambda p: pbar.update(1))
                        futures.append(future)
                        processing_times.append(time.time() - start)
                
                for future in as_completed(futures):
                    if future.exception():
                        logging.error(f"Error: {future.exception()}")

    end_time = time.time()
    logging.info(f"Thread Pool Method took: {end_time - start_time:.2f} seconds")
    processing_summary = log_processing_speeds(log_file, processing_times, counts)
    sort_log_by_filename(log_file, cbz_file_path, processing_summary)

# Method 2: Process Pool
def process_pool_method(zip_file_path, output_dir):
    log_file = 'metadata_extraction_method_2_process_pool.log'
    initialize_logging(log_file)

    start_time = time.time()
    processing_times = []
    counts = {'found': 0, 'not_found': 0, 'non_image': 0}

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        file_infos = sort_files_by_size(zip_ref)
        with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = []
            with tqdm(total=len(file_infos), desc="Process Pool Method", unit="file") as pbar:
                for file_info in file_infos:
                    if file_info.filename.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp', '.gif')):
                        start = time.time()
                        future = executor.submit(process_and_check_metadata, zip_file_path, file_info, output_dir, counts)
                        future.add_done_callback(lambda p: pbar.update(1))
                        futures.append(future)
                        processing_times.append(time.time() - start)
                
                for future in as_completed(futures):
                    if future.exception():
                        logging.error(f"Error: {future.exception()}")

    end_time = time.time()
    logging.info(f"Process Pool Method took: {end_time - start_time:.2f} seconds")
    processing_summary = log_processing_speeds(log_file, processing_times, counts)
    sort_log_by_filename(log_file, cbz_file_path, processing_summary)

# Method 3: Asynchronous I/O (asyncio + aiomultiprocess)
async def async_io_method(zip_file_path, output_dir):
    log_file = 'metadata_extraction_method_3_async_io.log'
    initialize_logging(log_file)

    start_time = time.time()
    processing_times = []
    counts = {'found': 0, 'not_found': 0, 'non_image': 0}

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        file_infos = sort_files_by_size(zip_ref)
        async with Pool() as pool:
            tasks = []
            # Ensure only one progress bar is initialized
            with tqdm(total=len(file_infos), desc="Async I/O Method", unit="file") as pbar:
                for file_info in file_infos:
                    if file_info.filename.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp', '.gif')):
                        start = time.time()
                        task = pool.apply(async_process_and_check_metadata, (zip_file_path, file_info, output_dir, counts))
                        tasks.append(task)
                        processing_times.append(time.time() - start)
                
                for task in asyncio.as_completed(tasks):
                    await task
                    pbar.update(1)

    end_time = time.time()
    logging.info(f"Asynchronous I/O Method took: {end_time - start_time:.2f} seconds")
    processing_summary = log_processing_speeds(log_file, processing_times, counts)
    sort_log_by_filename(log_file, cbz_file_path, processing_summary)

# Method 4: Manual Chunking + Async Processing
async def manual_chunking_method(zip_file_path, output_dir, chunk_size=100):
    log_file = 'metadata_extraction_method_4_manual_chunking.log'
    initialize_logging(log_file)

    start_time = time.time()
    processing_times = []
    counts = {'found': 0, 'not_found': 0, 'non_image': 0}

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        file_infos = sort_files_by_size(zip_ref)
        with tqdm(total=len(file_infos), desc="Manual Chunking Method", unit="file") as pbar:
            for i in range(0, len(file_infos), chunk_size):
                chunk = file_infos[i:i + chunk_size]
                tasks = []
                for file_info in chunk:
                    start = time.time()
                    tasks.append(async_process_and_check_metadata(zip_file_path, file_info, output_dir, counts))
                    processing_times.append(time.time() - start)
                for task in asyncio.as_completed(tasks):
                    await task
                    pbar.update(1)

    end_time = time.time()
    logging.info(f"Manual Chunking Method took: {end_time - start_time:.2f} seconds")
    processing_summary = log_processing_speeds(log_file, processing_times, counts)
    sort_log_by_filename(log_file, cbz_file_path, processing_summary)

# Main test function
def main():
    print("Testing Thread Pool Method")
    thread_pool_method(cbz_file_path, output_dir)

    print("Testing Process Pool Method")
    process_pool_method(cbz_file_path, output_dir)

    print("Testing Asynchronous I/O Method")
    asyncio.run(async_io_method(cbz_file_path, output_dir))

    print("Testing Manual Chunking Method")
    asyncio.run(manual_chunking_method(cbz_file_path, output_dir))

if __name__ == "__main__":
    main()
