import pandas as pd
import os
import sys
import re
import time
from datetime import datetime
from pathlib import Path
import requests
import xml.etree.ElementTree as ET

# Add project root to sys.path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from lib.config import Config

def get_concert_details_from_kopis(kopis_id: str) -> dict | None:
    """Fetches details for a single KOPIS ID and returns a dictionary of details."""
    if not kopis_id or pd.isna(kopis_id):
        return None

    api_key = Config.KOPIS_API_KEY
    url = f"http://www.kopis.or.kr/openApi/restful/pblprfr/{kopis_id}?service={api_key}"
    
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        
        root = ET.fromstring(response.text)
        db_element = root.find('.//db')
        
        if db_element is None:
            return None

        def _get_text(element, tag):
            found = element.find(tag)
            return found.text if found is not None and found.text else ""

        details = {
            'start_date': _get_text(db_element, 'prfpdfrom'),
            'end_date': _get_text(db_element, 'prfpdto'),
            'dtguidance': _get_text(db_element, 'dtguidance')
        }
        return details
    except Exception as e:
        print(f"  -> KOPIS API Error for {kopis_id}: {e}")
        return None

def populate_schedules():
    """
    Reads concerts.csv, fetches schedule details from KOPIS API,
    parses show times, and updates schedule.csv.
    """
    concerts_file = os.path.join(Config.OUTPUT_DIR, 'concerts.csv')
    schedule_file = os.path.join(Config.OUTPUT_DIR, 'schedule.csv')

    try:
        concerts_df = pd.read_csv(concerts_file, encoding='utf-8-sig')
    except FileNotFoundError:
        print(f"Error: {concerts_file} not found.")
        return

    new_schedule_rows = []

    print("Starting to populate schedules from concerts.csv...")
    day_map = {'월': 0, '화': 1, '수': 2, '목': 3, '금': 4, '토': 5, '일': 6}
    
    for index, concert in concerts_df.iterrows():
        concert_title = concert['title']
        concert_id = concert['id']
        kopis_id = concert['code']
        
        print(f"\nProcessing: {concert_title} (ID: {kopis_id})")

        if pd.isna(kopis_id):
            print("  -> Skipping, no KOPIS ID.")
            continue

        details = get_concert_details_from_kopis(kopis_id)
        time.sleep(0.1)

        if not details:
            print("  -> Could not fetch details from KOPIS.")
            continue

        dtguidance = details.get('dtguidance', '')
        start_date_str = details.get('start_date', '').replace('.', '-')
        end_date_str = details.get('end_date', '').replace('.', '-')

        if not dtguidance or not start_date_str or not end_date_str:
            print("  -> No dtguidance or date range found in API response.")
            continue

        # --- Final, Robust Parsing Logic ---
        schedule_rules = {}
        guidance = dtguidance.replace("요일", "").replace("(", " ").replace(")", " ")
        parts = re.split(r'\s*[/,·]\s*|\s*\n\s*', guidance)

        for part in parts:
            part = part.strip()
            if not part: continue

            times_in_part = re.findall(r'(\d{1,2}:\d{2})', part)
            if not times_in_part: continue

            days_to_apply = set()
            range_match = re.search(r'([월화수목금토일])\s*~\s*([월화수목금토일])', part)

            if range_match:
                start_day = day_map.get(range_match.group(1))
                end_day = day_map.get(range_match.group(2))
                if start_day is not None and end_day is not None:
                    for day_num in range(start_day, end_day + 1):
                        days_to_apply.add(day_num)
            
            if not days_to_apply:
                if '평일' in part or '주중' in part:
                    days_to_apply.update(range(0, 5))
                elif '주말' in part:
                    days_to_apply.update(range(5, 7))
                elif '매일' in part:
                    days_to_apply.update(range(0, 7))

            if not days_to_apply:
                found_days = re.findall(r'([월화수목금토일])', part)
                if found_days:
                    for day_char in found_days:
                        if day_char in day_map:
                            days_to_apply.add(day_map[day_char])

            if days_to_apply:
                for day_num in days_to_apply:
                    schedule_rules.setdefault(day_num, []).extend(times_in_part)
            else: # Fallback if no day info in this specific part
                for i in range(7):
                    schedule_rules.setdefault(i, []).extend(times_in_part)
        # --- End of Parsing Logic ---

        if not schedule_rules:
            print(f"  -> No time information found in dtguidance: '{dtguidance}'")
            continue

        try:
            s_date = pd.to_datetime(start_date_str).date()
            e_date = pd.to_datetime(end_date_str).date()
            date_range = pd.date_range(s_date, e_date)
        except Exception as e:
            print(f"  -> Error parsing date range: {e}. Defaulting to start_date only.")
            date_range = [pd.to_datetime(start_date_str).date()]

        num_days = len(date_range)
        print(f"  -> Found {num_days} day(s) and parsed rules for {len(schedule_rules)} day type(s).")

        for day_index, current_date in enumerate(date_range):
            day_of_week = current_date.dayofweek
            times_for_day = schedule_rules.get(day_of_week, [])

            if not times_for_day:
                continue

            category = f"{day_index + 1}일차 콘서트" if num_days > 1 else "콘서트"

            for found_time in times_for_day:
                try:
                    scheduled_at_dt = pd.to_datetime(f"{current_date.strftime('%Y-%m-%d')} {found_time}")
                    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                    new_row = {
                        'id': '',
                        'concert_id': concert_id,
                        'category': category,
                        'scheduled_at': scheduled_at_dt.strftime('%Y-%m-%d %H:%M:%S'),
                        'created_at': current_time,
                        'updated_at': current_time,
                        'type': 'CONCERT'
                    }
                    new_schedule_rows.append(new_row)
                    print(f"    -> Adding schedule: {category} on {current_date.strftime('%Y-%m-%d')} at {found_time}")
                except Exception as e:
                    print(f"    -> Error creating datetime: {e}")

    if not new_schedule_rows:
        print("\nNo new schedules to add.")
        return

    new_schedules_df = pd.DataFrame(new_schedule_rows)
    new_schedules_df.drop_duplicates(subset=['concert_id', 'scheduled_at'], keep='first', inplace=True)

    try:
        new_schedules_df.to_csv(schedule_file, index=False, encoding='utf-8-sig')
        print(f"\nSuccessfully saved {schedule_file} ({len(new_schedules_df)}개).")
    except Exception as e:
        print(f"\nError saving the file: {e}")

if __name__ == '__main__':
    populate_schedules()
