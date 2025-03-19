#!/usr/bin/env python3
import os
import json
import requests
import time
import re
import shutil
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Constants
OUTPUT_DIR = '/data/shared_hdd/netease'
ENGLISH_IDS_FILE = os.path.join(OUTPUT_DIR, 'english_song_ids_800k.txt')
PROCESSED_IDS_FILE = os.path.join(OUTPUT_DIR, 'processed_ids.txt')
GOOD_LYRICS_IDS_FILE = os.path.join(OUTPUT_DIR, 'good_lyrics_ids.txt')
LYRICS_DIR = os.path.join(OUTPUT_DIR, 'lyrics')
SONGS_DIR = os.path.join(OUTPUT_DIR, 'songs')
API_BASE_URL = 'http://localhost:3000'

# Lyric processing settings
MAX_WORKERS = 12
MIN_LYRIC_LENGTH = 100
MIN_ENGLISH_SEGMENTS = 6

# Song download settings
CHUNK_SIZE = 8192

# Shared settings
MAX_RETRIES = 3
RETRY_DELAY = 2

# Add these constants
BAD_LYRICS_IDS_FILE = os.path.join(OUTPUT_DIR, 'bad_lyrics_ids.txt')
NO_URL_IDS_FILE = os.path.join(OUTPUT_DIR, 'no_url_ids.txt')

# Add global tracking sets
bad_lyrics_ids = set()
no_url_ids = set()

# Ensure directories exist
os.makedirs(LYRICS_DIR, exist_ok=True)
os.makedirs(SONGS_DIR, exist_ok=True)

def get_song_lyric(song_id):
    """Fetch the lyrics for a song."""
    url = f"{API_BASE_URL}/lyric?id={song_id}"
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data['code'] == 200:
                    return data
                else:
                    return None
            else:
                print(f"Failed to fetch lyrics for song {song_id}, status: {response.status_code}")
        except Exception as e:
            print(f"Error fetching lyrics for song {song_id}: {e}")
        
        if attempt < MAX_RETRIES - 1:
            time.sleep(RETRY_DELAY)
    
    return None

def has_non_ascii_characters(text):
    """Check if the text contains any non-ASCII characters."""
    return any(ord(char) > 127 for char in text)

def is_good_lyric(lyric_data):
    """Check if the lyrics meet our criteria for being 'good'.
    Returns (bool, processed_lyrics) tuple where processed_lyrics contains
    only the good segments formatted with timestamps."""
    if not lyric_data or 'lrc' not in lyric_data or not lyric_data['lrc'].get('lyric'):
        return False, None
    
    # Get the main lyrics content
    lyric_text = lyric_data['lrc']['lyric']
    
    # Filter out short lyrics
    if len(lyric_text) < MIN_LYRIC_LENGTH:
        return False, None
    
    # Split lyrics by timestamp pattern [mm:ss.xx]
    segments = re.split(r'\[\d+:\d+\.\d+\]', lyric_text)
    timestamp_matches = re.findall(r'\[\d+:\d+\.\d+\]', lyric_text)
    
    # Pair timestamps with segments and remove empty segments
    paired_segments = []
    for i in range(min(len(segments), len(timestamp_matches))):
        if segments[i].strip():
            paired_segments.append((timestamp_matches[i], segments[i].strip()))
    
    # Filter out segments with non-ASCII characters
    ascii_segments = [(timestamp, text) for timestamp, text in paired_segments 
                     if not has_non_ascii_characters(text)]
    
    # Check if there are enough ASCII segments
    if len(ascii_segments) < MIN_ENGLISH_SEGMENTS:
        return False, None
    
    # Format the good segments back into lyric format
    processed_lyrics = '\n'.join(f"{timestamp}{text}" for timestamp, text in ascii_segments)
    return True, processed_lyrics

def get_song_url(song_id):
    """Fetch the download URL for a song."""
    # Skip if we already know this has no URL
    if song_id in no_url_ids:
        return None
        
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
                    # Add to no_url_ids set
                    no_url_ids.add(song_id)
                    return None
            else:
                print(f"Failed to fetch URL for song {song_id}, status: {response.status_code}")
        except Exception as e:
            print(f"Error fetching URL for song {song_id}: {e}")
        
        if attempt < MAX_RETRIES - 1:
            time.sleep(RETRY_DELAY)
    
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
            time.sleep(RETRY_DELAY)
    
    print(f"Failed to download song {song_id} after {MAX_RETRIES} attempts")
    return None

def save_bad_lyrics_ids():
    """Save the current set of bad lyrics IDs to file."""
    with open(BAD_LYRICS_IDS_FILE, 'w') as f:
        for song_id in bad_lyrics_ids:
            f.write(f"{song_id}\n")

def save_no_url_ids():
    """Save the current set of no URL IDs to file."""
    with open(NO_URL_IDS_FILE, 'w') as f:
        for song_id in no_url_ids:
            f.write(f"{song_id}\n")

def process_song(song_id):
    """Process a single song: fetch lyrics, check quality, and download if good."""
    # Check if we already know this has good lyrics
    lyric_path = os.path.join(LYRICS_DIR, f"{song_id}.txt")
    
    # If we already have lyrics, try to download the song
    if os.path.exists(lyric_path):
        # Check if song already exists
        song_exists = False
        for extension in ['mp3', 'm4a']:
            if os.path.exists(os.path.join(SONGS_DIR, f"{song_id}.{extension}")):
                song_exists = True
                break
        
        # Skip download if song exists or we know it has no URL
        if not song_exists and song_id not in no_url_ids:
            song_data = get_song_url(song_id)
            if song_data:
                download_song(song_data)
        return True
    
    # Check if we already know this has bad lyrics
    if song_id in bad_lyrics_ids:
        return False
    
    # Fetch and process lyrics
    lyric_data = get_song_lyric(song_id)
    if lyric_data:
        is_good, processed_lyrics = is_good_lyric(lyric_data)
        if is_good and processed_lyrics:
            # Save the processed lyric
            with open(lyric_path, 'w', encoding='utf-8') as f:
                f.write(processed_lyrics)
            
            # Immediately try to download the song if not in no_url_ids
            if song_id not in no_url_ids:
                song_data = get_song_url(song_id)
                if song_data:
                    download_song(song_data)
            return True
        else:
            # Mark as bad lyrics
            bad_lyrics_ids.add(song_id)
            return False
    
    # If we couldn't determine (API error, etc.), don't mark as bad
    return False

def combined_pipeline():
    """Run the combined pipeline for lyrics and song downloads."""
    if not os.path.exists(ENGLISH_IDS_FILE):
        print(f"Error: English song IDs file {ENGLISH_IDS_FILE} not found.")
        return False
    
    with open(ENGLISH_IDS_FILE, 'r') as f:
        all_song_ids = [line.strip() for line in f if line.strip()]
    
    print(f"Found {len(all_song_ids)} songs to process")
    
    # Load known bad lyrics IDs
    global bad_lyrics_ids, no_url_ids
    if os.path.exists(BAD_LYRICS_IDS_FILE):
        with open(BAD_LYRICS_IDS_FILE, 'r') as f:
            bad_lyrics_ids = {line.strip() for line in f if line.strip()}
    
    # Load known no URL IDs
    if os.path.exists(NO_URL_IDS_FILE):
        with open(NO_URL_IDS_FILE, 'r') as f:
            no_url_ids = {line.strip() for line in f if line.strip()}
    
    print(f"Found {len(bad_lyrics_ids)} songs with known bad lyrics")
    print(f"Found {len(no_url_ids)} songs with known unavailable URLs")
    
    # Count existing good lyrics by scanning directory
    existing_good_lyrics = {os.path.splitext(f)[0] for f in os.listdir(LYRICS_DIR) if f.endswith('.txt')}
    print(f"Found {len(existing_good_lyrics)} songs with good lyrics")
    
    # Get songs that have audio downloaded
    existing_audio = set()
    for extension in ['mp3', 'm4a']:
        existing_audio.update(os.path.splitext(f)[0] for f in os.listdir(SONGS_DIR) if f.endswith(extension))
    print(f"Found {len(existing_audio)} songs with audio downloaded")
    
    # A song is fully processed if it either:
    # 1. Has both lyrics and audio (good song)
    # 2. Is in bad_lyrics_ids (confirmed bad song)
    fully_processed = (existing_good_lyrics & existing_audio) | bad_lyrics_ids
    
    # Filter out songs that have been fully processed
    song_ids_to_process = [id for id in all_song_ids if id not in fully_processed]
    print(f"Remaining songs to process: {len(song_ids_to_process)}")
    
    success_count = 0
    failure_count = 0
    total_to_process = len(song_ids_to_process)
    
    print("\nProcessing songs...")
    
    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_song, song_id): song_id 
                      for song_id in song_ids_to_process}
            
            with tqdm(total=total_to_process, desc="Processing songs") as pbar:
                for future in as_completed(futures):
                    song_id = futures[future]
                    try:
                        result = future.result()
                        if result:
                            success_count += 1
                        else:
                            failure_count += 1
                        
                        # Update progress
                        pbar.update(1)
                        pbar.set_postfix(good=success_count, bad=failure_count)
                        
                        # Periodically save IDs
                        if (failure_count + success_count) % 100 == 0:
                            save_bad_lyrics_ids()
                            save_no_url_ids()
                    except Exception as e:
                        print(f"Error processing song {song_id}: {e}")
    
    except KeyboardInterrupt:
        print("\nInterrupted. Saving progress...")
    finally:
        # Save the final lists
        save_bad_lyrics_ids()
        save_no_url_ids()
    
    # Final count of good lyrics and audio
    final_good_lyrics = len([f for f in os.listdir(LYRICS_DIR) if f.endswith('.txt')])
    final_good_audio = len([f for f in os.listdir(SONGS_DIR) if f.endswith(('.mp3', '.m4a'))])
    
    print(f"\nResults:")
    print(f"- Found {final_good_lyrics} songs with good lyrics")
    print(f"- Found {final_good_audio} songs with audio downloaded")
    print(f"- Found {len(bad_lyrics_ids)} songs with bad lyrics")
    print(f"- Found {len(no_url_ids)} songs with unavailable URLs")
    print(f"- All lyrics saved to {LYRICS_DIR}")
    print(f"- All songs saved to {SONGS_DIR}")
    print(f"- Bad lyrics IDs saved to {BAD_LYRICS_IDS_FILE}")
    print(f"- No URL IDs saved to {NO_URL_IDS_FILE}")
    
    return True

def main():
    parser = argparse.ArgumentParser(description='Fetch lyrics and download songs in parallel')
    args = parser.parse_args()
    
    print("Starting combined lyrics and song download pipeline...")
    start_time = time.time()
    
    success = combined_pipeline()
    
    duration = time.time() - start_time
    print(f"\nCompleted in {duration:.2f} seconds")
    
    if success:
        print("Pipeline completed successfully!")
    else:
        print("Pipeline encountered errors.")
        return 1
    
    return 0

if __name__ == "__main__":
    main()