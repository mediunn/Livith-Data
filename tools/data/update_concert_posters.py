import pandas as pd
import os
import sys
import time
import requests
import xml.etree.ElementTree as ET
from pathlib import Path

# Add project root to sys.path to import Config
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from lib.config import Config

def get_poster_from_kopis(kopis_id: str) -> str | None:
    """Fetches details for a single KOPIS ID and returns the poster URL."""
    if not kopis_id or pd.isna(kopis_id):
        return None

    api_key = Config.KOPIS_API_KEY
    url = f"http://www.kopis.or.kr/openApi/restful/pblprfr/{kopis_id}?service={api_key}"
    
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        
        root = ET.fromstring(response.text)
        poster_element = root.find('.//db/poster')
        
        if poster_element is not None and poster_element.text:
            return poster_element.text
        return None
    except requests.RequestException as e:
        print(f"  -> API request failed for {kopis_id}: {e}")
        return None
    except ET.ParseError as e:
        print(f"  -> XML parsing failed for {kopis_id}: {e}")
        return None

def update_concert_posters():
    """
    Reads concerts.csv, fetches poster URLs for concerts with an empty 'poster' column,
    and saves the updated DataFrame back to the CSV.
    """
    concerts_file = os.path.join(Config.OUTPUT_DIR, 'concerts.csv')
    
    try:
        df = pd.read_csv(concerts_file, encoding='utf-8-sig')
        print(f"Successfully loaded {concerts_file}. Total concerts: {len(df)}")
    except FileNotFoundError:
        print(f"Error: {concerts_file} not found.")
        return

    if 'poster' not in df.columns:
        df['poster'] = ''
        print("Added 'poster' column.")

    # Fill NaN in 'poster' column to avoid issues with string checks
    df['poster'] = df['poster'].fillna('')

    updated_count = 0

    for index, row in df.iterrows():
        # Check if the poster field is empty
        if row.get('poster'):
            continue

        kopis_id = row.get('code')
        if not kopis_id or pd.isna(kopis_id):
            continue

        print(f"Processing concert: {row['title']} (ID: {kopis_id})")
        
        poster_url = get_poster_from_kopis(kopis_id)
        
        if poster_url:
            df.loc[index, 'poster'] = poster_url
            print(f"  -> Found poster URL: {poster_url}")
            updated_count += 1
        else:
            print(f"  -> Poster URL not found for {kopis_id}.")

        time.sleep(0.1)

    if updated_count > 0:
        try:
            df.to_csv(concerts_file, index=False, encoding='utf-8-sig')
            print(f"\nSuccessfully updated {updated_count} concert posters.")
            print(f"Saved updated data to {concerts_file}")
        except Exception as e:
            print(f"\nError saving the file: {e}")
    else:
        print("\nNo new poster URLs were added.")

if __name__ == '__main__':
    update_concert_posters()