import pandas as pd
import os

def fix_setlist_titles():
    base_path = '/Users/youz2me/Xcode/Livith-Data/output'
    concert_setlists_path = os.path.join(base_path, 'concert_setlists.csv')
    setlists_path = os.path.join(base_path, 'setlists.csv')
    
    concert_setlists_df = pd.read_csv(concert_setlists_path)
    setlists_df = pd.read_csv(setlists_path)
    
    for index, row in concert_setlists_df.iterrows():
        if pd.isna(row['setlist_title']) or row['setlist_title'] == '':
            concert_title = row['concert_title']
            setlist_type = row['type']
            
            if setlist_type == 'EXPECTED':
                new_title = f"{concert_title} 예상 셋리스트"
            elif setlist_type == 'PAST':
                new_title = f"{concert_title} 셋리스트"
            else:
                new_title = f"{concert_title} 셋리스트"
            
            concert_setlists_df.at[index, 'setlist_title'] = new_title
    
    concert_setlists_df.to_csv(concert_setlists_path, index=False, encoding='utf-8-sig')
    
    print("셋리스트 타이틀 채우기 완료:")
    print(concert_setlists_df[['concert_title', 'setlist_title', 'type']])
    
    print("\n기존 setlists.csv의 타이틀:")
    print(setlists_df['title'].tolist())
    
    print("\n업데이트된 concert_setlists.csv의 셋리스트 타이틀:")
    print(concert_setlists_df['setlist_title'].tolist())

if __name__ == "__main__":
    fix_setlist_titles()