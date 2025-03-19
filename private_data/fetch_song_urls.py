#!/usr/bin/env python3
import os
import json
import requests
import time
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Constants
OUTPUT_DIR = '/data/shared_hdd/netease'
GOOD_LYRICS_IDS_FILE = os.path.join(OUTPUT_DIR, 'good_lyrics_ids.txt')
URLS_FILE = os.path.join(OUTPUT_DIR, 'download_urls.json')
SONGS_DIR = os.path.join(OUTPUT_DIR, 'songs')
API_BASE_URL = 'http://localhost:3000'
MAX_WORKERS = 20  # Number of concurrent requests
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
CHUNK_SIZE = 8192  # bytes for streaming download

# Ensure directories exist
os.makedirs(SONGS_DIR, exist_ok=True)

def get_song_url(song_id):
    """Fetch the download URL for a song."""
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
    temp_path = f"{song_path}.tmp"
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, stream=True, timeout=30)
            
            if response.status_code == 200:
                total_size = int(response.headers.get('content-length', 0))
                
                with open(temp_path, 'wb') as f:
                    # Download without individual progress bar
                    for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)
                
                # Move the temporary file to the final destination
                shutil.move(temp_path, song_path)
                return song_path
            else:
                print(f"Failed to download song {song_id}, status: {response.status_code}")
        except Exception as e:
            print(f"Error downloading song {song_id}, attempt {attempt+1}/{MAX_RETRIES}: {str(e)}")
            if os.path.exists(temp_path):
                os.remove(temp_path)
        
        if attempt < MAX_RETRIES - 1:
            print(f"Retrying download for song {song_id} in {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)
    
    print(f"Failed to download song {song_id} after {MAX_RETRIES} attempts")
    return None

def process_song(song_id):
    """Process a single song: fetch URL, download song, and return metadata."""
    # First, fetch the URL
    song_data = get_song_url(song_id)
    if not song_data:
        return None
    
    # Then immediately download the song before the URL expires
    download_result = download_song(song_data)
    if not download_result:
        print(f"Warning: Failed to download song {song_id} even though URL was retrieved")
    
    # Return the song data for metadata storage
    return song_data

def fetch_and_download_songs():
    """Fetch URLs and immediately download songs for all songs with good lyrics."""
    # Load the good lyrics song IDs
    if not os.path.exists(GOOD_LYRICS_IDS_FILE):
        print(f"Error: Good lyrics song IDs file {GOOD_LYRICS_IDS_FILE} not found.")
        return None
    
    with open(GOOD_LYRICS_IDS_FILE, 'r') as f:
        song_ids = [line.strip() for line in f if line.strip()]
    
    print(f"Found {len(song_ids)} songs with good lyrics to process")
    
    # Check which songs we've already downloaded to avoid reprocessing
    processed_ids = set()
    for filename in os.listdir(SONGS_DIR):
        if filename.endswith('.mp3'):
            processed_ids.add(filename.split('.')[0])
    
    # Filter out songs that are already downloaded
    song_ids = [id for id in song_ids if id not in processed_ids]
    print(f"Remaining songs to download: {len(song_ids)}")
    
    if not song_ids:
        print("No new songs to download.")
        return []
    
    successful_urls = 0
    successful_downloads = 0
    failed_urls = 0
    songs_data = []
    
    # Process songs using thread pool
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit tasks to process songs (fetch URL and download)
        print("\nProcessing songs (fetching URLs and downloading)...")
        futures = {executor.submit(process_song, song_id): song_id for song_id in song_ids}
        
        # Process results with progress bar
        for future in tqdm(as_completed(futures), total=len(song_ids), desc="Processing songs"):
            song_id = futures[future]
            try:
                song_data = future.result()
                if song_data:
                    songs_data.append(song_data)
                    successful_urls += 1
                    successful_downloads += 1
                else:
                    failed_urls += 1
            except Exception as e:
                print(f"Error processing song {song_id}: {e}")
                failed_urls += 1
    
    # Save the URLs data for reference
    if songs_data:
        try:
            with open(URLS_FILE, 'w', encoding='utf-8') as f:
                json.dump(songs_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Error saving URLs data: {e}")
    
    print(f"\nResults:")
    print(f"- Successfully fetched {successful_urls} download URLs")
    print(f"- Successfully downloaded {successful_downloads} songs")
    print(f"- Failed to process {failed_urls} songs")
    print(f"- URLs saved to {URLS_FILE}")
    print(f"- Songs saved to {SONGS_DIR}")
    
    return songs_data

def main():
    """Main function to fetch song URLs and download songs."""
    print("Starting to fetch song URLs and download songs...")
    
    # Fetch URLs and download songs
    fetch_and_download_songs()
    
    print("\nProcessing completed.")

if __name__ == "__main__":
    main()