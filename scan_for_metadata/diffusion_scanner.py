import os
import hashlib
from enum import Enum
from typing import List, Optional, Dict, Any
from PIL import Image
from PIL.ExifTags import TAGS
from PIL.PngImagePlugin import PngImageFile

# Global variable for verbose output, default to False
VERBOSE_OUTPUT = False

def set_verbose_output(value: bool):
    """
    Sets the global VERBOSE_OUTPUT flag to enable or disable verbose output.
    
    Parameters:
    value (bool): If True, enables verbose output; if False, disables verbose output.
    """
    global VERBOSE_OUTPUT
    VERBOSE_OUTPUT = value

class FileParameters:
    def __init__(self, path: str):
        self.path = path
        self.prompt: Optional[str] = None
        self.negative_prompt: Optional[str] = None
        self.steps: Optional[int] = None
        self.sampler: Optional[str] = None
        self.cfg_scale: Optional[float] = None
        self.seed: Optional[int] = None
        self.width: Optional[int] = None
        self.height: Optional[int] = None
        self.model_hash: Optional[str] = None
        self.model: Optional[str] = None
        self.batch_size: Optional[int] = None
        self.batch_pos: Optional[int] = None
        self.other_parameters: Optional[str] = None
        self.parameters: Optional[str] = None
        self.aesthetic_score: Optional[float] = None
        self.hyper_network: Optional[str] = None
        self.hyper_network_strength: Optional[float] = None
        self.clip_skip: Optional[int] = None
        self.ensd: Optional[int] = None
        self.prompt_strength: Optional[float] = None
        self.file_size: Optional[int] = None
        self.no_metadata: bool = False

class HashFunctions:
    @staticmethod
    def calculate_hash(file_path: str) -> str:
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            f.seek(0x100000)
            buffer = f.read(0x10000)
            hash_md5.update(buffer)
        return hash_md5.hexdigest()[:8]

    @staticmethod
    def calculate_sha256(file_path: str) -> str:
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256.update(chunk)
        return sha256.hexdigest()

class MetaFormat(Enum):
    A1111 = "A1111"
    NovelAI = "NovelAI"
    InvokeAI = "InvokeAI"
    EasyDiffusion = "EasyDiffusion"
    Fooocus = "Fooocus"
    StableDiffusion = "Stable Diffusion"  # Added Stable Diffusion format
    Unknown = "Unknown"

class FileType(Enum):
    PNG = "PNG"
    JPEG = "JPEG"
    WebP = "WebP"
    Other = "Other"

class ImageGenerationMetadata:
    PNG_MAGIC = b'\x89PNG'
    JPEG_MAGIC = b'\xFF\xD8\xFF'
    WEBP_MAGIC = b'RIFF'

    @staticmethod
    def get_file_type(file_path: str) -> FileType:
        with open(file_path, 'rb') as f:
            header = f.read(12)
            if header.startswith(ImageGenerationMetadata.PNG_MAGIC):
                return FileType.PNG
            elif header.startswith(ImageGenerationMetadata.JPEG_MAGIC):
                return FileType.JPEG
            elif header[:4] == ImageGenerationMetadata.WEBP_MAGIC and header[8:12] == b'WEBP':
                return FileType.WebP
            else:
                return FileType.Other

    @staticmethod
    def read_metadata(file_path: str) -> FileParameters:
        global VERBOSE_OUTPUT
        file_parameters = FileParameters(path=file_path)
        file_type = ImageGenerationMetadata.get_file_type(file_path)

        if VERBOSE_OUTPUT:
            print(f"Scanning {file_path}...")

        if file_type == FileType.PNG:
            file_parameters = ImageGenerationMetadata._read_png_metadata(file_path)
        elif file_type == FileType.JPEG:
            file_parameters = ImageGenerationMetadata._read_jpeg_metadata(file_path)

        file_parameters.file_size = os.path.getsize(file_path)
        return file_parameters

    @staticmethod
    def _read_png_metadata(file_path: str) -> FileParameters:
        file_parameters = FileParameters(path=file_path)

        try:
            image = Image.open(file_path)
            if isinstance(image, PngImageFile):
                for key, value in image.info.items():
                    if isinstance(value, bytes):
                        value = value.decode('utf-8', errors='replace')
                    if key.lower() in ["parameters", "prompt", "workflow"]:
                        file_parameters.parameters = value

        except Exception as e:
            print(f"Error reading PNG metadata from {file_path}: {e}")

        return file_parameters

    @staticmethod
    def _read_jpeg_metadata(file_path: str) -> FileParameters:
        file_parameters = FileParameters(path=file_path)

        try:
            image = Image.open(file_path)
            exif_data = image._getexif()

            if exif_data is not None:
                for tag, value in exif_data.items():
                    decoded_tag = TAGS.get(tag, tag)
                    if decoded_tag == "UserComment":
                        user_comment = value.decode("utf-8", errors='replace')
                        file_parameters.prompt = user_comment

        except Exception as e:
            print(f"Error reading EXIF metadata from {file_path}: {e}")

        return file_parameters

    @staticmethod
    def contains_image_generation_metadata(file_path: str) -> bool:
        """
        Returns True if the file contains any image generation metadata.
        """
        try:
            file_parameters = ImageGenerationMetadata.read_metadata(file_path)
            has_metadata = any([
                file_parameters.prompt,
                file_parameters.negative_prompt,
                file_parameters.sampler,
                file_parameters.seed,
                file_parameters.cfg_scale,
                file_parameters.model,
                file_parameters.model_hash,
                file_parameters.width,
                file_parameters.height,
                file_parameters.parameters  # Check for custom ComfyUI parameters
            ])

            if VERBOSE_OUTPUT:
                if has_metadata:
                    print(f"Metadata found in {file_path}.")
                else:
                    print(f"No metadata found in {file_path}.")

            return has_metadata
        except Exception as e:
            print(f"Error checking metadata for {file_path}: {e}")
            return False

class MetadataScanner:
    @staticmethod
    def get_files(directory: str, extensions: str, ignore_files: Optional[set] = None, recursive: bool = True, exclude_paths: Optional[List[str]] = None) -> List[str]:
        files = []
        for extension in extensions.split(','):
            if recursive:
                for root, _, filenames in os.walk(directory):
                    if exclude_paths and any(root.startswith(p) for p in exclude_paths):
                        continue
                    files.extend([os.path.join(root, f) for f in filenames if f.endswith(extension)])
            else:
                files.extend([os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(extension)])

        if ignore_files:
            files = [f for f in files if f not in ignore_files]

        return files

    @staticmethod
    def get_file(directory: str, extensions: str, ignore_files: Optional[set] = None, recursive: bool = True, exclude_paths: Optional[List[str]] = None) -> Optional[str]:
        """
        Returns the first file that matches the given extensions and criteria.
        """
        files = MetadataScanner.get_files(directory, extensions, ignore_files, recursive, exclude_paths)
        return files[0] if files else None

    @staticmethod
    def scan(files: List[str]) -> List[FileParameters]:
        scanned_files = []
        for file_path in files:
            try:
                file_params = ImageGenerationMetadata.read_metadata(file_path)
                scanned_files.append(file_params)
            except Exception as e:
                print(f"Error scanning {file_path}: {e}")
        return scanned_files

class ModelScanner:
    @staticmethod
    def scan(directory: str) -> List[Dict[str, Any]]:
        models = []
        for root, _, filenames in os.walk(directory):
            for filename in filenames:
                if filename.endswith(('.ckpt', '.safetensors')):
                    file_path = os.path.join(root, filename)
                    relative_path = os.path.relpath(file_path, directory)
                    try:
                        file_hash = HashFunctions.calculate_hash(file_path)
                    except Exception as e:
                        file_hash = None
                        print(f"Error hashing {file_path}: {e}")
                    models.append({
                        "path": relative_path,
                        "filename": os.path.splitext(filename)[0],
                        "hash": file_hash,
                        "is_local": True
                    })
        return models
