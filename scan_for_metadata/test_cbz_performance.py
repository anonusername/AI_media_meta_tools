import time
import zipfile
import os
import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from aiomultiprocess import Pool
import logging
from tqdm import tqdm
import subprocess

# Set up logging
logging.basicConfig(filename='metadata_extraction.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Define the CBZ file path and output directory
cbz_file_path = 'path/to/large.cbz'
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

# Sort files by size for better load balancing
def sort_files_by_size(zip_ref):
    return sorted(zip_ref.infolist(), key=lambda f: f.file_size, reverse=True)

# Method 1: Thread Pool
def thread_pool_method(zip_file_path, output_dir):
    start_time = time.time()

    def process_and_check_metadata(zip_ref, file_info, output_dir):
        try:
            with zip_ref.open(file_info.filename) as image_file:
                temp_path = os.path.join(output_dir, file_info.filename)
                with open(temp_path, 'wb') as temp_image:
                    temp_image.write(image_file.read())
                if check_metadata(temp_path):
                    logging.info(f"Keyword found in {file_info.filename}")
                os.remove(temp_path)
        except Exception as e:
            logging.error(f"Failed to process {file_info.filename}: {e}")

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        file_infos = sort_files_by_size(zip_ref)
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = []
            with tqdm(total=len(file_infos), desc="Thread Pool Method", unit="file") as pbar:
                for file_info in file_infos:
                    if file_info.filename.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp', '.gif')):
                        future = executor.submit(process_and_check_metadata, zip_ref, file_info, output_dir)
                        future.add_done_callback(lambda p: pbar.update(1))
                        futures.append(future)
                
                for future in as_completed(futures):
                    if future.exception():
                        logging.error(f"Error: {future.exception()}")

    end_time = time.time()
    logging.info(f"Thread Pool Method took: {end_time - start_time:.2f} seconds")

# Method 2: Process Pool
def process_pool_method(zip_file_path, output_dir):
    start_time = time.time()

    def process_and_check_metadata(zip_file_path, file_info, output_dir):
        try:
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                with zip_ref.open(file_info.filename) as image_file:
                    temp_path = os.path.join(output_dir, file_info.filename)
                    with open(temp_path, 'wb') as temp_image:
                        temp_image.write(image_file.read())
                    if check_metadata(temp_path):
                        logging.info(f"Keyword found in {file_info.filename}")
                    os.remove(temp_path)
        except Exception as e:
            logging.error(f"Failed to process {file_info.filename}: {e}")

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        file_infos = sort_files_by_size(zip_ref)
        with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = []
            with tqdm(total=len(file_infos), desc="Process Pool Method", unit="file") as pbar:
                for file_info in file_infos:
                    if file_info.filename.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp', '.gif')):
                        future = executor.submit(process_and_check_metadata, zip_file_path, file_info, output_dir)
                        future.add_done_callback(lambda p: pbar.update(1))
                        futures.append(future)
                
                for future in as_completed(futures):
                    if future.exception():
                        logging.error(f"Error: {future.exception()}")

    end_time = time.time()
    logging.info(f"Process Pool Method took: {end_time - start_time:.2f} seconds")

# Method 3: Asynchronous I/O (asyncio + aiomultiprocess)
async def async_io_method(zip_file_path, output_dir):
    start_time = time.time()

    async def process_and_check_metadata(zip_file_path, file_info, output_dir):
        try:
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                with zip_ref.open(file_info.filename) as image_file:
                    temp_path = os.path.join(output_dir, file_info.filename)
                    with open(temp_path, 'wb') as temp_image:
                        temp_image.write(image_file.read())
                    if check_metadata(temp_path):
                        logging.info(f"Keyword found in {file_info.filename}")
                    os.remove(temp_path)
        except Exception as e:
            logging.error(f"Failed to process {file_info.filename}: {e}")

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        file_infos = sort_files_by_size(zip_ref)
        async with Pool() as pool:
            tasks = []
            with tqdm(total=len(file_infos), desc="Async I/O Method", unit="file") as pbar:
                for file_info in file_infos:
                    if file_info.filename.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp', '.gif')):
                        tasks.append(pool.apply(process_and_check_metadata, (zip_file_path, file_info, output_dir)))
                for task in asyncio.as_completed(tasks):
                    await task
                    pbar.update(1)

    end_time = time.time()
    logging.info(f"Asynchronous I/O Method took: {end_time - start_time:.2f} seconds")

# Method 4: Manual Chunking + Async Processing
async def manual_chunking_method(zip_file_path, output_dir, chunk_size=100):
    start_time = time.time()

    async def process_and_check_metadata(zip_ref, file_info, output_dir):
        try:
            with zip_ref.open(file_info.filename) as image_file:
                temp_path = os.path.join(output_dir, file_info.filename)
                with open(temp_path, 'wb') as temp_image:
                    temp_image.write(image_file.read())
                if check_metadata(temp_path):
                    logging.info(f"Keyword found in {file_info.filename}")
                os.remove(temp_path)
        except Exception as e:
            logging.error(f"Failed to process {file_info.filename}: {e}")

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        file_infos = sort_files_by_size(zip_ref)
        with tqdm(total=len(file_infos), desc="Manual Chunking Method", unit="file") as pbar:
            for i in range(0, len(file_infos), chunk_size):
                chunk = file_infos[i:i + chunk_size]
                tasks = [process_and_check_metadata(zip_ref, file_info, output_dir) for file_info in chunk]
                for task in asyncio.as_completed(tasks):
                    await task
                    pbar.update(1)

    end_time = time.time()
    logging.info(f"Manual Chunking Method took: {end_time - start_time:.2f} seconds")

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
