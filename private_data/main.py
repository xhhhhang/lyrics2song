#!/usr/bin/env python3
import os
import sys
import time
import subprocess
import argparse

def run_script(script_name, description):
    """Run a script and return whether it was successful."""
    print(f"\n{'='*80}")
    print(f"PHASE: {description}")
    print(f"{'='*80}\n")
    
    start_time = time.time()
    result = subprocess.run(['python3', script_name], check=False)
    duration = time.time() - start_time
    
    print(f"\nCompleted {description} in {duration:.2f} seconds")
    
    if result.returncode != 0:
        print(f"ERROR: {script_name} exited with code {result.returncode}")
        return False
    return True

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run Netease music download pipeline')
    parser.add_argument('--start-phase', type=int, default=1, choices=[1, 2, 3],
                      help='Start from phase number (1: Extract IDs, 2: Fetch Lyrics, 3: Fetch URLs and Download)')
    parser.add_argument('--combined', action='store_true',
                     help='Use the combined pipeline for lyrics and song downloads')
    args = parser.parse_args()

    # Make sure output directory exists
    os.makedirs('/data/shared_hdd/netease', exist_ok=True)
    
    # Use combined pipeline if requested
    if args.combined:
        print("\n" + "="*80)
        print("RUNNING COMBINED PIPELINE")
        print("="*80 + "\n")
        
        if args.start_phase <= 1:
            if not run_script('get_english_songs.py', "Extracting English song IDs"):
                sys.exit(1)
        
        if not run_script('combined_pipeline.py', "Running combined lyrics and song download pipeline"):
            sys.exit(2)
        
        print("\n" + "="*80)
        print("Combined pipeline completed successfully!")
        print("Songs with good lyrics have been downloaded to /data/shared_hdd/netease")
        print("="*80)
        return
    
    # Phase 1: Extract English song IDs
    if args.start_phase <= 1:
        if not run_script('get_english_songs.py', "Extracting English song IDs"):
            sys.exit(1)
    
    # Phase 2: Fetch and filter lyrics
    if args.start_phase <= 2:
        if not run_script('fetch_lyrics.py', "Fetching and filtering lyrics"):
            sys.exit(2)
    
    # Phase 3: Fetch song URLs and download songs for songs with good lyrics
    if args.start_phase <= 3:
        if not run_script('fetch_song_urls.py', "Fetching URLs and downloading songs with good lyrics"):
            sys.exit(3)
    
    print("\n" + "="*80)
    print("All phases completed successfully!")
    print("Songs with good lyrics have been downloaded to /data/shared_hdd/netease")
    print("="*80)

if __name__ == "__main__":
    main() 