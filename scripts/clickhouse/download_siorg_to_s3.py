import os
import re
import sys
import shutil
import zipfile
import requests
import boto3
from dotenv import load_dotenv

# Load configuration from .env file
load_dotenv()

# S3 Configuration
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
bucket_name = os.getenv("S3_BUCKET_NAME", "unb-bdm-siorg")

# Safety checks
if not aws_access_key or "YOUR_ACCESS_KEY_ID_HERE" in aws_access_key:
    print("Error: AWS credentials are not configured in the .env file.")
    sys.exit(1)

# Base Repository URLs
REPO_BASE = "https://repositorio.dados.gov.br/seges/siorg/"
CATEGORIES = {
    "estrutura-organizacional-completa": {
        "url": REPO_BASE + "estrutura-organizacional-completa/",
        "pattern": r'href="([^"]+completa-\d{4}-\d{2}\.zip)"',
        "s3_folder": "estrutura-organizacional-completa"
    },
    "distribuicao": {
        "url": REPO_BASE + "distribuicao/",
        "pattern": r'href="([^"]+siorg-\d{4}-\d{2}\.zip)"',
        "s3_folder": "distribuicao"
    }
}

# Local temporary download directory
TEMP_DIR = "temp_downloads"

def initialize_s3():
    """Initialize S3 client"""
    return boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region
    )

def fetch_available_files(category_name, config):
    """Fetch the list of available ZIP files from the repository directory"""
    print(f"\nFetching list of files for category '{category_name}'...")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
    }
    try:
        response = requests.get(config["url"], headers=headers, timeout=20)
        if response.status_code != 200:
            print(f"Failed to fetch list from {config['url']}. Status code: {response.status_code}")
            return []
        
        # Parse links using regex
        files = re.findall(config["pattern"], response.text)
        # Remove duplicates and sort
        files = sorted(list(set(files)))
        return files
    except Exception as e:
        print(f"Error fetching file list: {e}")
        return []

def download_file(url, local_path):
    """Download a file with progress indicator"""
    print(f"Downloading {url}...")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
    }
    response = requests.get(url, headers=headers, stream=True, timeout=30)
    response.raise_for_status()
    
    total_size = int(response.headers.get('content-length', 0))
    block_size = 1024 * 1024 # 1 MB
    downloaded = 0
    
    with open(local_path, 'wb') as f:
        for data in response.iter_content(block_size):
            f.write(data)
            downloaded += len(data)
            if total_size > 0:
                percent = (downloaded / total_size) * 100
                print(f"Progress: {percent:.1f}% ({downloaded / (1024*1024):.1f}MB / {total_size / (1024*1024):.1f}MB)", end="\r")
            else:
                print(f"Progress: {downloaded / (1024*1024):.1f}MB downloaded", end="\r")
    print("\nDownload finished.")

def upload_to_s3(s3_client, local_path, s3_key):
    """Upload file to S3"""
    print(f"Uploading '{local_path}' to S3 as '{s3_key}'...")
    try:
        # Using S3 upload_file which automatically handles multi-part uploads if file is large
        s3_client.upload_file(local_path, bucket_name, s3_key)
        print("Upload completed successfully.")
        return True
    except Exception as e:
        print(f"Failed to upload to S3: {e}")
        return False

def main():
    print("=" * 60)
    print("           SIORG DATASET DOWNLOADER TO AWS S3")
    print("=" * 60)
    
    # Options menu
    print("\nSelect an option:")
    print("1. Download and upload the LATEST file of each category (Recommended)")
    print("2. Download and upload files for a SPECIFIC month (format: YYYY-MM)")
    print("3. Download and upload ALL historical files (Warning: ~800MB total)")
    
    choice = input("\nEnter choice (1, 2, or 3): ").strip()
    
    target_month = None
    latest_only = False
    
    if choice == "1" or choice == "":
        latest_only = True
        print("\nSelected: Download latest available files.")
    elif choice == "2":
        target_month = input("Enter month (e.g., 2026-07): ").strip()
        if not re.match(r"^\d{4}-\d{2}$", target_month):
            print("Invalid format. Must be YYYY-MM.")
            sys.exit(1)
        print(f"\nSelected: Download files for {target_month}.")
    elif choice == "3":
        print("\nSelected: Download ALL historical files.")
    else:
        print("Invalid choice.")
        sys.exit(1)
        
    # Ensure local temp dir exists
    os.makedirs(TEMP_DIR, exist_ok=True)
    
    try:
        s3_client = initialize_s3()
        # Test bucket access quickly
        s3_client.get_bucket_location(Bucket=bucket_name)
    except Exception as e:
        print(f"\nFailed to connect to S3. Please run 'python scripts/clickhouse/test_s3_connection.py' first.")
        print(f"Error: {e}")
        sys.exit(1)
        
    for category_name, config in CATEGORIES.items():
        available_files = fetch_available_files(category_name, config)
        if not available_files:
            print(f"No files found for category {category_name}.")
            continue
            
        # Filter files depending on choice
        files_to_process = []
        if latest_only:
            files_to_process = [available_files[-1]] # The last item is the most recent
        elif target_month:
            files_to_process = [f for f in available_files if target_month in f]
            if not files_to_process:
                print(f"No file matching {target_month} found in category {category_name}.")
                print(f"Available files range from {available_files[0]} to {available_files[-1]}.")
        else:
            files_to_process = available_files
            
        print(f"Files to process in '{category_name}': {files_to_process}")
        
        for filename in files_to_process:
            file_url = config["url"] + filename
            local_path = os.path.join(TEMP_DIR, filename)
            
            try:
                # 1. Download ZIP file
                download_file(file_url, local_path)
                
                # 2. Create temporary extraction folder
                extracted_dir = os.path.join(TEMP_DIR, filename.replace(".zip", ""))
                os.makedirs(extracted_dir, exist_ok=True)
                
                print(f"Extracting {local_path} to {extracted_dir}...")
                with zipfile.ZipFile(local_path, "r") as zf:
                    zf.extractall(extracted_dir)
                
                # List extracted files
                extracted_files = [f for f in os.listdir(extracted_dir) if os.path.isfile(os.path.join(extracted_dir, f))]
                print(f"Extracted files: {extracted_files}")
                
                # 3. Upload extracted files to S3
                for ext_file in extracted_files:
                    local_ext_path = os.path.join(extracted_dir, ext_file)
                    
                    # If there's exactly 1 file in the ZIP (which is the case for SIORG files),
                    # rename it in S3 to match the ZIP name (replacing .zip with the CSV extension) to keep the YYYY-MM date!
                    if len(extracted_files) == 1:
                        _, ext = os.path.splitext(ext_file)
                        new_name = filename.replace(".zip", ext)
                        s3_key = f"{config['s3_folder']}/{new_name}"
                    else:
                        # If there are multiple files, upload them under a subfolder matching the ZIP name
                        zip_folder_name = filename.replace(".zip", "")
                        s3_key = f"{config['s3_folder']}/{zip_folder_name}/{ext_file}"
                        
                    upload_to_s3(s3_client, local_ext_path, s3_key)
                
                # 4. Clean up temporary downloaded ZIP and extracted folder
                if os.path.exists(local_path):
                    os.remove(local_path)
                if os.path.exists(extracted_dir):
                    shutil.rmtree(extracted_dir)
                    print(f"Cleaned up extracted folder: {extracted_dir}")
                    
            except Exception as ex:
                print(f"Error processing {filename}: {ex}")
                if os.path.exists(local_path):
                    os.remove(local_path)
                if 'extracted_dir' in locals() and os.path.exists(extracted_dir):
                    shutil.rmtree(extracted_dir)
                    
    # Clean up temp folder if empty
    try:
        if os.path.exists(TEMP_DIR) and not os.listdir(TEMP_DIR):
            os.rmdir(TEMP_DIR)
    except Exception:
        pass
        
    print("\nProcess finished.")

if __name__ == "__main__":
    main()

