import subprocess
import sys
import os
import time
import zipfile
import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from tqdm import tqdm
import shutil

# Utility function to check for keywords in image metadata using ExifTool
def check_metadata(image_file_path):
    try:
        # Run ExifTool command to extract metadata
        result = subprocess.run(
            ['exiftool', image_file_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        metadata = result.stdout
        # Check if any specific keywords are present in the metadata
        if any(word in metadata.lower() for word in ["model", "lora", "negative", "prompt"]):
            return True
    except Exception as e:
        print(f"Failed to run ExifTool on {image_file_path}: {e}")
    return False

# Synchronous function to process and check metadata in an image file within a CBZ
def process_and_check_metadata(zip_file_path, file_info, output_dir, counts):
    try:
        # Open the CBZ file and extract the specific image file
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            with zip_ref.open(file_info.filename) as image_file:
                # Prefix the filename with the CBZ name to avoid naming conflicts
                prefixed_filename = f"{os.path.basename(zip_file_path)}_{file_info.filename}"
                temp_path = os.path.join(output_dir, prefixed_filename)
                
                # Write the extracted image to a temporary file
                with open(temp_path, 'wb') as temp_image:
                    temp_image.write(image_file.read())
                
                # Check for specific metadata in the image file
                if check_metadata(temp_path):
                    counts['found'] += 1
                else:
                    counts['not_found'] += 1
                
                # Remove the temporary file after checking
                os.remove(temp_path)
    except Exception as e:
        print(f"Failed to process {file_info.filename}: {e}")
        counts['non_image'] += 1

# Asynchronous version of process_and_check_metadata
async def async_process_and_check_metadata(zip_file_path, file_info, output_dir, counts):
    try:
        # Use asyncio to run the synchronous function in a non-blocking manner
        return await asyncio.to_thread(process_and_check_metadata, zip_file_path, file_info, output_dir, counts)
    except Exception as e:
        print(f"Failed to process {file_info.filename}: {e}")
        counts['non_image'] += 1

# Function to sort files by size within the CBZ for better load balancing
def sort_files_by_size(zip_ref):
    return sorted(zip_ref.infolist(), key=lambda f: f.file_size, reverse=True)

# Function to clear the output directory before processing a new CBZ file
def clear_output_directory(output_dir):
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)

# Method 1: Thread Pool for processing CBZ files
def thread_pool_method(cbz_files, output_dir):
    method_start_time = time.time()
    print("\n====================================================\n")
    for zip_file_path in cbz_files:
        print(f"Testing Thread Pool Method {os.path.basename(zip_file_path)}")
        clear_output_directory(output_dir)
        start_time = time.time()
        counts = {'found': 0, 'not_found': 0, 'non_image': 0}

        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            file_infos = sort_files_by_size(zip_ref)
            with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
                futures = []
                with tqdm(total=len(file_infos), desc=f"Thread Pool Method ({os.path.basename(zip_file_path)})", unit="file") as pbar:
                    for file_info in file_infos:
                        if file_info.filename.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp', '.gif', '.webp')):
                            future = executor.submit(process_and_check_metadata, zip_file_path, file_info, output_dir, counts)
                            future.add_done_callback(lambda p: pbar.update(1))
                            futures.append(future)
                    
                    for future in as_completed(futures):
                        if future.exception():
                            print(f"Error: {future.exception()}")

        end_time = time.time()
        print(f"> Thread Pool Method for {os.path.basename(zip_file_path)} took: {end_time - start_time:.2f} seconds")

    method_end_time = time.time()
    print(f">> Total time for Thread Pool Method: {method_end_time - method_start_time:.2f} seconds <<")

# Method 2: Process Pool for processing CBZ files
def process_pool_method(cbz_files, output_dir):
    method_start_time = time.time()
    print("\n====================================================\n")
    for zip_file_path in cbz_files:
        print(f"Testing Process Pool Method {os.path.basename(zip_file_path)}")
        clear_output_directory(output_dir)
        start_time = time.time()
        counts = {'found': 0, 'not_found': 0, 'non_image': 0}

        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            file_infos = sort_files_by_size(zip_ref)
            with ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
                futures = []
                with tqdm(total=len(file_infos), desc=f"Process Pool Method ({os.path.basename(zip_file_path)})", unit="file") as pbar:
                    for file_info in file_infos:
                        if file_info.filename.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp', '.gif', '.webp')):
                            future = executor.submit(process_and_check_metadata, zip_file_path, file_info, output_dir, counts)
                            future.add_done_callback(lambda p: pbar.update(1))
                            futures.append(future)
                    
                    for future in as_completed(futures):
                        if future.exception():
                            print(f"Error: {future.exception()}")

        end_time = time.time()
        print(f"> Process Pool Method for {os.path.basename(zip_file_path)} took: {end_time - start_time:.2f} seconds")

    method_end_time = time.time()
    print(f">> Total time for Process Pool Method: {method_end_time - method_start_time:.2f} seconds <<")

# Method 3: Asynchronous I/O for processing CBZ files
async def async_io_method(cbz_files, output_dir):
    method_start_time = time.time()
    print("\n====================================================\n")
    for zip_file_path in cbz_files:
        print(f"Testing Asynchronous I/O Method {os.path.basename(zip_file_path)}")
        clear_output_directory(output_dir)
        start_time = time.time()
        counts = {'found': 0, 'not_found': 0, 'non_image': 0}

        # Semaphore to limit the number of concurrent tasks
        semaphore = asyncio.Semaphore(10)
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            file_infos = sort_files_by_size(zip_ref)
            tasks = []
            for file_info in file_infos:
                if file_info.filename.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp', '.gif', '.webp')):
                    tasks.append(asyncio.create_task(async_process_and_check_metadata(zip_file_path, file_info, output_dir, counts)))

            with tqdm(total=len(file_infos), desc=f"Async I/O Method ({os.path.basename(zip_file_path)})", unit="file") as pbar:
                for task in asyncio.as_completed(tasks):
                    await task
                    pbar.update(1)

        end_time = time.time()
        print(f"> Asynchronous I/O Method for {os.path.basename(zip_file_path)} took: {end_time - start_time:.2f} seconds")

    method_end_time = time.time()
    print(f">> Total time for Asynchronous I/O Method: {method_end_time - method_start_time:.2f} seconds <<")

# Method 4: Manual Chunking + Async Processing for processing CBZ files
async def manual_chunking_method(cbz_files, output_dir, chunk_size=100):
    method_start_time = time.time()
    print("\n====================================================\n")
    for zip_file_path in cbz_files:
        print(f"Testing Manual Chunking Method {os.path.basename(zip_file_path)}")
        clear_output_directory(output_dir)
        start_time = time.time()
        counts = {'found': 0, 'not_found': 0, 'non_image': 0}

        # Semaphore to limit the number of concurrent tasks
        semaphore = asyncio.Semaphore(10)
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            file_infos = sort_files_by_size(zip_ref)
            with tqdm(total=len(file_infos), desc=f"Manual Chunking Method ({os.path.basename(zip_file_path)})", unit="file") as pbar:
                for i in range(0, len(file_infos), chunk_size):
                    chunk = file_infos[i:i + chunk_size]
                    chunk_tasks = []
                    for file_info in chunk:
                        if file_info.filename.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp', '.gif', '.webp')):
                            chunk_tasks.append(asyncio.create_task(async_process_and_check_metadata(zip_file_path, file_info, output_dir, counts)))
                    
                    # Await all tasks in the current chunk
                    await asyncio.gather(*chunk_tasks)
                    pbar.update(len(chunk))

        end_time = time.time()
        print(f"> Manual Chunking Method for {os.path.basename(zip_file_path)} took: {end_time - start_time:.2f} seconds")

    method_end_time = time.time()
    print(f">> Total time for Manual Chunking Method: {method_end_time - method_start_time:.2f} seconds <<")


# Main test function to execute all methods
def main():
    if len(sys.argv) < 2:
        print("Usage: python test_cbz_performance.py <cbz_file_path_1> <cbz_file_path_2> ... <cbz_file_path_n>")
        sys.exit(1)

    cbz_files = sys.argv[1:]
    output_dir = 'output_directory'
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Run all methods sequentially
    thread_pool_method(cbz_files, output_dir)
    process_pool_method(cbz_files, output_dir)
    asyncio.run(async_io_method(cbz_files, output_dir))
    asyncio.run(manual_chunking_method(cbz_files, output_dir))

if __name__ == "__main__":
    main()
