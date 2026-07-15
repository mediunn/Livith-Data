import pandas as pd
import os
import sys
from pathlib import Path

# Add project root to sys.path to import Config
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))
from lib.config import Config

def verify_artist_ids():
    """
    Compares artist_id in concerts.csv against the correct id in artists.csv
    to find mismatches.
    """
    concerts_file = os.path.join(Config.OUTPUT_DIR, 'concerts.csv')
    artists_file = os.path.join(Config.OUTPUT_DIR, 'artists.csv')

    try:
        concerts_df = pd.read_csv(concerts_file, encoding='utf-8-sig')
        artists_df = pd.read_csv(artists_file, encoding='utf-8-sig')
        print(f"Successfully loaded {os.path.basename(concerts_file)} and {os.path.basename(artists_file)}.")
    except FileNotFoundError as e:
        print(f"Error: Could not find file - {e.filename}")
        return

    # Create a mapping of artist name to their correct ID
    artists_df.dropna(subset=['artist', 'id'], inplace=True)
    artist_name_to_id_map = pd.Series(artists_df.id.values, index=artists_df.artist).to_dict()
    print(f"Created a map of {len(artist_name_to_id_map)} artists to their IDs.")

    mismatched_concerts = []

    # Iterate through concerts and verify the artist_id
    for index, concert in concerts_df.iterrows():
        artist_name = concert.get('artist')
        concert_artist_id = concert.get('artist_id')

        if pd.isna(artist_name) or not artist_name or pd.isna(concert_artist_id):
            continue

        correct_artist_id = artist_name_to_id_map.get(artist_name)

        if correct_artist_id is not None:
            if int(concert_artist_id) != int(correct_artist_id):
                mismatched_concerts.append({
                    'Concert Title': concert.get('title'),
                    'Artist Name': artist_name,
                    'Expected ID': int(correct_artist_id),
                    'Actual ID': int(concert_artist_id)
                })

    if mismatched_concerts:
        print("\n--- Found Artist ID Mismatches ---")
        print("The 'artist_id' in concerts.csv does not match the 'id' in artists.csv for the following concerts:")
        for mismatch in mismatched_concerts:
            print(f"  - Concert: '{mismatch['Concert Title']}' by '{mismatch['Artist Name']}'")
            print(f"    -> Expected artist_id: {mismatch['Expected ID']}, but found: {mismatch['Actual ID']}")
    else:
        print("\n--- No Artist ID Mismatches Found ---")
        print("The 'artist_id' in all concerts correctly corresponds to the artist's 'id'.")

if __name__ == '__main__':
    verify_artist_ids()
