#!/usr/bin/env python3
import os
import json
import requests
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Constants
OUTPUT_DIR = '/data/shared_hdd/netease'
ENGLISH_IDS_FILE = os.path.join(OUTPUT_DIR, 'english_song_ids_800k.txt')
GOOD_LYRICS_IDS_FILE = os.path.join(OUTPUT_DIR, 'good_lyrics_ids.txt')
LYRICS_DIR = os.path.join(OUTPUT_DIR, 'lyrics')
API_BASE_URL = 'http://localhost:3000'
MAX_WORKERS = 8  # Number of concurrent requests
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
MIN_LYRIC_LENGTH = 100  # Minimum characters for a "good" lyric
MIN_ENGLISH_SEGMENTS = 6  # Minimum number of English segments required
SKIP_NEW = False

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
                    print(f"No lyrics available for song {song_id}")
                    return None
            else:
                print(f"Failed to fetch lyrics for song {song_id}, status: {response.status_code}")
        except Exception as e:
            print(f"Error fetching lyrics for song {song_id}: {e}")
        
        if attempt < MAX_RETRIES - 1:
            print(f"Retrying lyrics for song {song_id} in {RETRY_DELAY} seconds...")
            time.sleep(RETRY_DELAY)
    
    print(f"Failed to fetch lyrics for song {song_id} after {MAX_RETRIES} attempts")
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

def fetch_and_filter_lyrics():
    """Fetch lyrics for all English songs and filter out songs with short lyrics."""
    # Load the English song IDs
    if not os.path.exists(ENGLISH_IDS_FILE):
        print(f"Error: English song IDs file {ENGLISH_IDS_FILE} not found.")
        return None
    
    with open(ENGLISH_IDS_FILE, 'r') as f:
        song_ids = [line.strip() for line in f if line.strip()]
    
    print(f"Found {len(song_ids)} songs to process for lyrics")
    
    # Create directory for lyrics
    os.makedirs(LYRICS_DIR, exist_ok=True)
    
    # Check which lyrics we already have
    existing_lyrics = set()
    for lyric_file in os.listdir(LYRICS_DIR):
        if lyric_file.endswith('.json') or lyric_file.endswith('.txt'):
            existing_lyrics.add(lyric_file.split('.')[0])
    
    # Filter out song IDs that we've already processed
    song_ids = [id for id in song_ids if id not in existing_lyrics]
    print(f"Remaining songs to fetch lyrics for: {len(song_ids)}")
    
    # Fetch lyrics using thread pool
    successful_lyrics = len(existing_lyrics)
    good_lyrics_ids = []
    
    # First, check existing lyrics for quality
    for song_id in existing_lyrics:
        good_lyrics_ids.append(song_id)
    print(f"Found {len(good_lyrics_ids)} songs with good lyrics from existing files")

    if SKIP_NEW:
        print("Skipping new lyrics...")
        print(f"Found {len(good_lyrics_ids)} songs with good lyrics from existing files")
        with open(GOOD_LYRICS_IDS_FILE, 'w', encoding='utf-8') as f:
            for song_id in good_lyrics_ids:
                f.write(f"{song_id}\n")
        return good_lyrics_ids
    
    # Fetch new lyrics and filter
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit lyric fetch tasks
        print("\nFetching new lyrics...")
        lyric_futures = {executor.submit(get_song_lyric, song_id): song_id for song_id in song_ids}
        
        # Process lyric results with progress bar
        for future in tqdm(as_completed(lyric_futures), total=len(song_ids), desc="Fetching lyrics"):
            song_id = lyric_futures[future]
            try:
                lyric_data = future.result()
                if lyric_data:
                    # Check if lyrics meet our criteria
                    is_good, processed_lyrics = is_good_lyric(lyric_data)
                    if is_good:
                        # Save just the processed lyric text
                        txt_file = os.path.join(LYRICS_DIR, f"{song_id}.txt")
                        with open(txt_file, 'w', encoding='utf-8') as f:
                            f.write(processed_lyrics)
                        good_lyrics_ids.append(song_id)
                    
                    successful_lyrics += 1
            except Exception as e:
                print(f"Error processing lyrics result for song {song_id}: {e}")
    
    # Save the list of song IDs with good lyrics
    print(f"\nSaving {len(good_lyrics_ids)} songs with good lyrics...")
    with open(GOOD_LYRICS_IDS_FILE, 'w', encoding='utf-8') as f:
        for song_id in good_lyrics_ids:
            f.write(f"{song_id}\n")
    
    print(f"\nLyrics Results:")
    print(f"- Successfully fetched {successful_lyrics} lyrics")
    print(f"- Found {len(good_lyrics_ids)} songs with good lyrics")
    print(f"- Good lyrics IDs saved to {GOOD_LYRICS_IDS_FILE}")
    print(f"- All lyrics saved to {LYRICS_DIR}")
    
    return good_lyrics_ids

def main():
    """Main function to fetch and filter lyrics."""
    print("Starting to fetch and filter song lyrics...")
    fetch_and_filter_lyrics()
    print("\nLyrics fetching and filtering completed.")

if __name__ == "__main__":
    main() 