#!/bin/bash

# Script Name: gather_loose_images_to_cbz.sh

# Description:
# This script traverses a given directory tree and processes directories 
# containing image files. Specifically, it:
# 1. Searches through the provided base directory (and its subdirectories) 
#    for folders containing at least one supported image file type.
# 2. If a folder contains supported image files (with extensions such as 
#    .png, .jpg, .jpeg, .webp, or .gif), the script will archive the contents 
#    of that folder into a .zip file.
# 3. The resulting .zip file is then renamed to a .cbz file, which is a 
#    common format for comic book archives.
# 4. After successfully creating the .cbz file, the script deletes the 
#    original directory and its contents to clean up.
# 5. The script ensures that necessary tools (`find`, `zip`, and `rm`) are 
#    installed before proceeding.
#
# Usage:
#   ./gather_loose_images_to_cbz.sh /path/to/base_directory
#
#   - The script takes one argument, which is the path to the base directory 
#     where the search and processing should begin.
#   - Ensure the script has execute permissions before running it:
#     chmod +x gather_loose_images_to_cbz.sh
#
# Example:
#   ./gather_loose_images_to_cbz.sh /home/user/comics
#
# Notes:
#   - This script assumes that only the directories containing image files 
#     should be archived. Directories without image files or empty directories 
#     are skipped.
#   - The script can handle directories containing non-image files but will 
#     only create archives if there are supported image files present.

# Supported image file extensions
EXTENSIONS=("png" "jpg" "jpeg" "webp" "gif")

# Function to check if required tools are installed
check_tools() {
    local tools=("find" "zip" "rm")
    for tool in "${tools[@]}"; do
        if ! command -v "$tool" &> /dev/null; then
            echo "Error: '$tool' is not installed or not in the PATH."
            exit 1
        fi
    done
}

# Function to check if a directory contains at least one supported image file
contains_images() {
    local dir="$1"
    shopt -s nullglob # Handle empty directories gracefully

    local image_count=0

    for file in "$dir"/*; do
        # Skip if it's not a file
        [ -f "$file" ] || continue

        # Extract the file extension (case-insensitive)
        ext="${file##*.}"
        ext="${ext,,}" # Convert to lowercase

        # Check if the file is an image
        if [[ " ${EXTENSIONS[*]} " =~ " ${ext} " ]]; then
            ((image_count++))
        fi
    done

    # Return true (0) if there is at least one image file
    if [ "$image_count" -gt 0 ]; then
        return 0
    else
        return 1
    fi
}

# Main function to traverse directories and archive them
process_directories() {
    local base_dir="$1"

    # Traverse through all directories
    find "$base_dir" -type d | while IFS= read -r dir; do
        # Skip if the directory is empty
        if [ -z "$(ls -A "$dir")" ]; then
            continue
        fi

        if contains_images "$dir"; then
            zip_name="${dir}.zip"
            cbz_name="${dir}.cbz"

            echo "Zipping '$dir' into '$zip_name'..."
            (cd "$dir" && zip -r "../$(basename "$zip_name")" .)
            mv "$zip_name" "$cbz_name"
            echo "Renamed '$zip_name' to '$cbz_name'"

            # Delete the original directory
            rm -rf "$dir"
            echo "Deleted original directory '$dir'"
        fi
    done
}

# Usage help
usage() {
    echo "Usage: $0 directory"
    echo "  directory       The base directory to start processing from."
    exit 1
}

# Check for required tools
check_tools

# Check for directory argument
if [ -z "$1" ]; then
    usage
fi

# Run the main function with the provided arguments
process_directories "$1"
