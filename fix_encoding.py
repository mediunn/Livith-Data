import pandas as pd
import os
from config import Config

def fix_csv_encoding():
    """기존 CSV 파일들의 인코딩을 수정"""
    output_dir = Config.OUTPUT_DIR
    
    if not os.path.exists(output_dir):
        print("출력 디렉토리가 존재하지 않습니다.")
        return
    
    csv_files = [f for f in os.listdir(output_dir) if f.endswith('.csv')]
    
    if not csv_files:
        print("CSV 파일이 없습니다.")
        return
    
    print(f"{len(csv_files)}개의 CSV 파일을 처리합니다: {csv_files}")
    
    for csv_file in csv_files:
        filepath = os.path.join(output_dir, csv_file)
        backup_filepath = filepath + '.backup'
        
        try:
            # 기존 파일 백업
            os.rename(filepath, backup_filepath)
            
            # 다양한 인코딩으로 읽기 시도
            df = None
            successful_encoding = None
            encodings = ['utf-8', 'utf-8-sig', 'cp949', 'euc-kr', 'latin1']
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(backup_filepath, encoding=encoding)
                    successful_encoding = encoding
                    print(f"{csv_file}: {encoding} 인코딩으로 읽기 성공")
                    break
                except Exception as e:
                    print(f"{csv_file}: {encoding} 인코딩 실패 - {str(e)[:50]}...")
                    continue
            
            if df is not None:
                # UTF-8-sig로 다시 저장
                df.to_csv(
                    filepath,
                    index=False,
                    encoding='utf-8-sig',
                    escapechar='\\',
                    quoting=1
                )
                print(f"{csv_file}: UTF-8-sig로 재저장 완료")
                
                # 저장 검증
                try:
                    test_df = pd.read_csv(filepath, encoding='utf-8-sig')
                    print(f"{csv_file}: 저장 검증 성공 ({len(test_df)}행)")
                except Exception as e:
                    print(f"{csv_file}: 저장 검증 실패 - {e}")
                
                # 백업 파일 삭제
                os.remove(backup_filepath)
            else:
                # 복원
                os.rename(backup_filepath, filepath)
                print(f"{csv_file}: 모든 인코딩 시도 실패, 원본 복원")
                
        except Exception as e:
            print(f"{csv_file} 처리 중 오류: {e}")
            # 백업이 있다면 복원
            if os.path.exists(backup_filepath):
                if os.path.exists(filepath):
                    os.remove(filepath)
                os.rename(backup_filepath, filepath)
                print(f"{csv_file}: 오류로 인한 원본 복원")

if __name__ == "__main__":
    fix_csv_encoding()
