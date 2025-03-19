#!/usr/bin/env python3
import os
import json
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import shutil
from tqdm import tqdm

# Constants
OUTPUT_DIR = '/data/shared_hdd/netease'
URLS_FILE = os.path.join(OUTPUT_DIR, 'download_urls_checkpoint.json')
SONGS_DIR = os.path.join(OUTPUT_DIR, 'songs')
MAX_WORKERS = 10
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
CHUNK_SIZE = 8192  # bytes for streaming download

# Create songs directory if it doesn't exist
os.makedirs(SONGS_DIR, exist_ok=True)

def download_song(song_data):
    """Download a song using its URL."""
    song_id = song_data['id']
    url = song_data['url']
    file_type = song_data.get('type', 'mp3')
    song_path = os.path.join(SONGS_DIR, f"{song_id}.{file_type}")
    
    # Skip if already downloaded
    if os.path.exists(song_path):
        file_size = os.path.getsize(song_path)
        expected_size = song_data.get('size')
        
        # If file exists and size matches (or size is unknown), skip download
        if expected_size is None or file_size == expected_size:
            return song_path
        else:
            # Remove the file if size doesn't match
            os.remove(song_path)
    
    # Download the file
    for attempt in range(MAX_RETRIES):
        try:
            print(f"Downloading: {url}")
            response = requests.get(url, stream=True, timeout=30)
            
            if response.status_code == 200:
                total_size = int(response.headers.get('content-length', 0))
                
                # Create a temporary file for downloading
                temp_path = f"{song_path}.tmp"
                
                with open(temp_path, 'wb') as f, tqdm(
                    desc=f"Downloading {song_id}",
                    total=total_size,
                    unit='B',
                    unit_scale=True,
                    unit_divisor=1024,
                    leave=False  # Don't leave the progress bar after completion
                ) as bar:
                    for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)
                            bar.update(len(chunk))
                
                # Move the temporary file to the final destination
                shutil.move(temp_path, song_path)
                return song_path
        except Exception as e:
            print(f"Error downloading song {song_id}, attempt {attempt+1}/{MAX_RETRIES}: {str(e)}")
            if os.path.exists(f"{song_path}.tmp"):
                os.remove(f"{song_path}.tmp")
        
        if attempt < MAX_RETRIES - 1:
            time.sleep(RETRY_DELAY)
    
    print(f"Failed to download song {song_id} after {MAX_RETRIES} attempts")
    return None

def get_song_url(song_id):
    """Fetch a fresh download URL for a song that failed previously."""
    API_BASE_URL = 'http://localhost:3000'
    url = f"{API_BASE_URL}/song/url?id={song_id}"
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data['code'] == 200 and data['data'] and data['data'][0]['url']:
                    song_data = data['data'][0]
                    return {
                        'id': song_id,
                        'url': song_data['url'],
                        'size': song_data.get('size'),
                        'type': song_data.get('type'),
                        'br': song_data.get('br')
                    }
                else:
                    print(f"No URL available for song {song_id}")
                    return None
            else:
                print(f"Failed to fetch URL for song {song_id}, status: {response.status_code}")
        except Exception as e:
            print(f"Error fetching URL for song {song_id}: {e}")
        
        if attempt < MAX_RETRIES - 1:
            print(f"Retrying song {song_id} in {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)
    
    print(f"Failed to fetch URL for song {song_id} after {MAX_RETRIES} attempts")
    return None

def refresh_and_download(song_id):
    """Refresh the URL and download a song, used for songs with expired URLs."""
    fresh_song_data = get_song_url(song_id)
    if not fresh_song_data:
        return None
    
    return download_song(fresh_song_data)

def download_missing_songs():
    """Check for songs in the URLs file that haven't been downloaded yet and try to download them."""
    if not os.path.exists(URLS_FILE):
        print(f"Error: URLs file {URLS_FILE} not found. Run fetch_song_urls.py first.")
        return
    
    with open(URLS_FILE, 'r') as f:
        songs_data = json.load(f)
    
    # Check which songs need to be downloaded
    songs_to_download = []
    missing_songs = []
    
    for song_data in songs_data:
        song_id = song_data['id']
        file_type = song_data.get('type', 'mp3')
        song_path = os.path.join(SONGS_DIR, f"{song_id}.{file_type}")
        
        if not os.path.exists(song_path):
            missing_songs.append(song_id)
            songs_to_download.append(song_data)
    
    print(f"Checking {len(songs_data)} songs:")
    print(f"- Missing songs to download: {len(missing_songs)}")
    
    if not missing_songs:
        print("No missing songs to download.")
        return
    
    # Try to download from existing URLs first
    successful_downloads = 0
    failed_downloads = []
    
    print("\nAttempting to download missing songs with existing URLs...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_song, song_data): song_data['id'] for song_data in songs_to_download}
        
        for future in tqdm(as_completed(futures), total=len(songs_to_download), desc="Downloading songs"):
            song_id = futures[future]
            try:
                result = future.result()
                if result:
                    successful_downloads += 1
                else:
                    failed_downloads.append(song_id)
            except Exception:
                failed_downloads.append(song_id)
    
    # For failed downloads (likely due to expired URLs), try refreshing the URL
    if failed_downloads:
        print(f"\n{len(failed_downloads)} songs failed due to expired URLs. Refreshing URLs and retrying...")
        refreshed_successful = 0
        still_failed = 0
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(refresh_and_download, song_id): song_id for song_id in failed_downloads}
            
            for future in tqdm(as_completed(futures), total=len(failed_downloads), desc="Refreshing and downloading"):
                song_id = futures[future]
                try:
                    result = future.result()
                    if result:
                        refreshed_successful += 1
                    else:
                        still_failed += 1
                except Exception:
                    still_failed += 1
        
        print(f"- Successfully downloaded after URL refresh: {refreshed_successful}")
        print(f"- Still failed after URL refresh: {still_failed}")
        successful_downloads += refreshed_successful
    
    print(f"\nDownload completed:")
    print(f"- Successfully downloaded: {successful_downloads} out of {len(missing_songs)}")
    print(f"- Songs saved to {SONGS_DIR}")

if __name__ == "__main__":
    print("Starting to check and download missing songs...")
    download_missing_songs()
    print("Process completed.") 