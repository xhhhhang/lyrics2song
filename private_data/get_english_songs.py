#!/usr/bin/env python3
import os
import json
import glob
from tqdm import tqdm

# Define the paths
METADATA_PATH = '/renhangx/lyrics2song/data/metadata_package'
OUTPUT_DIR = '/data/shared_hdd/netease'
ENGLISH_IDS_FILE = os.path.join(OUTPUT_DIR, 'english_song_ids.txt')

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

def process_metadata_files():
    """Extract song IDs that have English as their language."""
    english_ids = []
    
    # Get all metadata files
    metadata_files = []
    for batch_dir in glob.glob(os.path.join(METADATA_PATH, 'batch*')):
        metadata_dir = os.path.join(batch_dir, 'metadata')
        metadata_files.extend(glob.glob(os.path.join(metadata_dir, '*.json')))
    
    print("Processing metadata files to find English songs...")
    
    # Process each metadata JSON file
    for json_file in tqdm(metadata_files, desc="Processing metadata files"):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                # Load the entire JSON file
                data = json.load(f)
                
                # Iterate through all songs in the data
                for song_id, song_info in data.items():
                    # Check if the language is English
                    languages = song_info.get('language', [])
                    if isinstance(languages, list) and '英语' in languages:
                        english_ids.append(song_id)
        except Exception as e:
            # Print error but continue with other files
            print(f"Error processing {json_file}: {str(e)}")
            continue
    
    # Save the English song IDs to a file
    with open(ENGLISH_IDS_FILE, 'w') as f:
        for song_id in english_ids:
            f.write(f"{song_id}\n")
    
    print(f"\nFound {len(english_ids)} English songs. IDs saved to {ENGLISH_IDS_FILE}")
    return english_ids

if __name__ == "__main__":
    print("Starting to extract English song IDs...")
    process_metadata_files()
    print("Extraction completed.") 