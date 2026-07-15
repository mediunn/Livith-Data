import pandas as pd
import os
import sys
from pathlib import Path

# Add project root to sys.path to import Config
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))
from lib.config import Config

def find_orphan_concerts():
    """
    Compares concerts.csv and artists.csv to find concerts
    whose artist does not exist in the artists table.
    """
    concerts_file = os.path.join(Config.OUTPUT_DIR, 'concerts.csv')
    artists_file = os.path.join(Config.OUTPUT_DIR, 'artists.csv')

    try:
        concerts_df = pd.read_csv(concerts_file, encoding='utf-8-sig')
        artists_df = pd.read_csv(artists_file, encoding='utf-8-sig')
        print(f"Successfully loaded {os.path.basename(concerts_file)} ({len(concerts_df)} rows) and {os.path.basename(artists_file)} ({len(artists_df)} rows).")
    except FileNotFoundError as e:
        print(f"Error: Could not find file - {e.filename}")
        return

    # Create a set of valid artist names for quick lookup
    valid_artist_names = set(artists_df['artist'].dropna().unique())
    print(f"Found {len(valid_artist_names)} unique artists in artists.csv.")

    orphan_concerts = []
    
    # Iterate through concerts and check if the artist exists
    for index, concert in concerts_df.iterrows():
        artist_name = concert.get('artist')
        
        if pd.isna(artist_name) or not artist_name:
            continue

        if artist_name not in valid_artist_names:
            orphan_concerts.append({
                'Concert Title': concert.get('title'),
                'Artist Name': artist_name
            })

    if orphan_concerts:
        print("\n--- Found Orphan Concerts ---")
        print("The following concerts have an artist that does not exist in artists.csv:")
        for orphan in orphan_concerts:
            print(f"  - Concert: '{orphan['Concert Title']}', Artist: '{orphan['Artist Name']}'")
    else:
        print("\n--- No Orphan Concerts Found ---")
        print("All artists in concerts.csv exist in artists.csv.")

if __name__ == '__main__':
    find_orphan_concerts()
