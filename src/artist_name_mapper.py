#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""아티스트 이름 매핑 시스템 - 한국어와 원어 이름을 상호 변환"""

import re
from typing import Optional, Dict, Tuple, List

class ArtistNameMapper:
    """아티스트 이름을 한국어와 원어로 매핑하는 클래스"""
    
    # 자주 사용되는 아티스트 매핑 테이블
    ARTIST_MAPPING = {
        # K-POP
        "방탄소년단": "BTS",
        "블랙핑크": "BLACKPINK", 
        "뉴진스": "NewJeans",
        "르세라핌": "LE SSERAFIM",
        "아이브": "IVE",
        "에스파": "aespa",
        "있지": "ITZY",
        "트와이스": "TWICE",
        "레드벨벳": "Red Velvet",
        "스트레이 키즈": "Stray Kids",
        "세븐틴": "SEVENTEEN",
        "엔시티": "NCT",
        "엑소": "EXO",
        "아이유": "IU",
        "태연": "TAEYEON",
        
        # 해외 아티스트
        "뮤즈": "Muse",
        "콜드플레이": "Coldplay",
        "라디오헤드": "Radiohead",
        "아크틱 몽키즈": "Arctic Monkeys",
        "그린 데이": "Green Day",
        "레드 핫 칠리 페퍼스": "Red Hot Chili Peppers",
        "레드 핫 칠리 페퍼즈": "Red Hot Chili Peppers",
        "마룬 5": "Maroon 5",
        "마룬5": "Maroon 5",
        "원디렉션": "One Direction",
        "빌리 아일리시": "Billie Eilish",
        "아리아나 그란데": "Ariana Grande",
        "테일러 스위프트": "Taylor Swift",
        "에드 시런": "Ed Sheeran",
        "브루노 마스": "Bruno Mars",
        "샘 스미스": "Sam Smith",
        "듀아 리파": "Dua Lipa",
        "위켄드": "The Weeknd",
        "더 위켄드": "The Weeknd",
        "이매진 드래곤스": "Imagine Dragons",
        "원 리퍼블릭": "OneRepublic",
        "원리퍼블릭": "OneRepublic",
        "케이티 페리": "Katy Perry",
        "레이디 가가": "Lady Gaga",
        "리한나": "Rihanna",
        
        # 일본 아티스트
        "원오크록": "ONE OK ROCK",
        "원 오크 록": "ONE OK ROCK",
        "엑스 재팬": "X JAPAN",
        "라르크 앙 시엘": "L'Arc-en-Ciel",
        "아지카스": "AJICUS",
        "히토리에": "Hitorie",
        "요네즈 겐시": "Yonezu Kenshi",
        "아도": "Ado",
        "리사": "LiSA",
        "우타다 히카루": "Utada Hikaru",
        "아유미 하마사키": "Ayumi Hamasaki",
        
        # 인디/얼터너티브
        "녹황색사회": "녹황색사회",  # 한국 인디밴드
        "잠비나이": "Jambinai",
        "혁오": "HYUKOH",
        "실리카겔": "Silica Gel",
        "검정치마": "The Black Skirts",
        "장기하와 얼굴들": "Kiha & The Faces",
        
        # 클래식/재즈
        "알 디 메올라": "Al Di Meola",
        "키스 자렛": "Keith Jarrett",
        "마일스 데이비스": "Miles Davis",
        "존 콜트레인": "John Coltrane",
        "빌 에반스": "Bill Evans",
        "패트 메스니": "Pat Metheny",
        "차이콥스키": "Tchaikovsky",
        "베토벤": "Beethoven",
        "모차르트": "Mozart",
        "바흐": "Bach",
        
        # 특수 케이스 (괄호 표기)
        "뮤즈 (MUSE)": "Muse",
        "콜드플레이 (Coldplay)": "Coldplay",
        "방탄소년단 (BTS)": "BTS",
    }
    
    @classmethod
    def get_search_names(cls, artist_name: str) -> Tuple[str, str]:
        """
        아티스트 이름으로부터 검색용 한국어명과 원어명을 반환
        
        Args:
            artist_name: 입력 아티스트명
            
        Returns:
            Tuple[str, str]: (한국어명, 원어명)
        """
        if not artist_name or not artist_name.strip():
            return "", ""
            
        # 입력값 정리
        clean_name = artist_name.strip()
        
        # 1. 괄호 표기 분석 (예: "뮤즈 (MUSE)", "BTS (방탄소년단)")
        korean_name, english_name = cls._parse_parentheses_format(clean_name)
        if korean_name and english_name:
            return korean_name, english_name
            
        # 2. 매핑 테이블에서 검색
        mapped_name = cls._find_in_mapping(clean_name)
        if mapped_name:
            # 한국어 → 영어 매핑인지 확인
            if clean_name in cls.ARTIST_MAPPING:
                return clean_name, mapped_name
            # 영어 → 한국어 매핑인지 확인 (역방향)
            else:
                reverse_mapping = {v: k for k, v in cls.ARTIST_MAPPING.items()}
                if clean_name in reverse_mapping:
                    return reverse_mapping[clean_name], clean_name
        
        # 3. 언어 추정 (한글 포함 여부로 판단)
        if cls._contains_korean(clean_name):
            # 한국어로 추정 → 원어 검색 시도
            return clean_name, cls._guess_english_name(clean_name)
        else:
            # 영어로 추정 → 한국어 검색은 빈 문자열
            return "", clean_name
    
    @classmethod
    def get_optimal_search_name(cls, artist_name: str) -> str:
        """
        가장 효과적인 검색용 이름 반환 (주로 영어명 우선)
        
        Args:
            artist_name: 입력 아티스트명
            
        Returns:
            str: 최적 검색명
        """
        korean_name, english_name = cls.get_search_names(artist_name)
        
        # 영어명이 있으면 영어명 우선 (해외 검색에 유리)
        if english_name and english_name.strip():
            return english_name.strip()
        
        # 영어명이 없으면 한국어명
        if korean_name and korean_name.strip():
            return korean_name.strip()
            
        # 둘 다 없으면 원본
        return artist_name.strip()
    
    @classmethod 
    def _parse_parentheses_format(cls, name: str) -> Tuple[str, str]:
        """
        괄호 표기 분석 (예: "뮤즈 (MUSE)", "BTS (방탄소년단)")
        
        Returns:
            Tuple[str, str]: (한국어명, 영어명)
        """
        # 괄호 패턴 매칭
        pattern = r'^(.+?)\s*\(([^)]+)\)$'
        match = re.search(pattern, name.strip())
        
        if not match:
            return "", ""
            
        main_part = match.group(1).strip()
        bracket_part = match.group(2).strip()
        
        # 메인 부분이 한글이면 한국어명, 괄호 부분이 영어명
        if cls._contains_korean(main_part) and not cls._contains_korean(bracket_part):
            return main_part, bracket_part
        
        # 메인 부분이 영어이고 괄호 부분이 한글이면 반대
        if not cls._contains_korean(main_part) and cls._contains_korean(bracket_part):
            return bracket_part, main_part
            
        return "", ""
    
    @classmethod
    def _find_in_mapping(cls, name: str) -> Optional[str]:
        """매핑 테이블에서 검색"""
        # 직접 매칭
        if name in cls.ARTIST_MAPPING:
            return cls.ARTIST_MAPPING[name]
            
        # 역방향 매칭 (값에서 키 찾기)
        reverse_mapping = {v: k for k, v in cls.ARTIST_MAPPING.items()}
        if name in reverse_mapping:
            return reverse_mapping[name]
            
        # 대소문자 무시 매칭
        name_lower = name.lower()
        for key, value in cls.ARTIST_MAPPING.items():
            if key.lower() == name_lower or value.lower() == name_lower:
                return value if key.lower() == name_lower else key
                
        return None
    
    @classmethod
    def _contains_korean(cls, text: str) -> bool:
        """한글 포함 여부 확인"""
        if not text:
            return False
        return bool(re.search(r'[가-힣]', text))
    
    @classmethod
    def _guess_english_name(cls, korean_name: str) -> str:
        """한국어 아티스트명으로부터 영어명 추정"""
        # 매핑 테이블에 있으면 반환
        if korean_name in cls.ARTIST_MAPPING:
            return cls.ARTIST_MAPPING[korean_name]
            
        # 일반적인 변환 규칙들 (매우 기초적)
        # 실제로는 이 부분은 API나 데이터베이스 조회로 대체해야 함
        common_patterns = {
            "밴드": "Band",
            "그룹": "Group", 
            "오케스트라": "Orchestra",
            "앙상블": "Ensemble"
        }
        
        result = korean_name
        for kor, eng in common_patterns.items():
            if kor in result:
                result = result.replace(kor, eng)
                
        # 변환이 일어났으면 반환, 아니면 빈 문자열
        return result if result != korean_name else ""

    @classmethod
    def add_mapping(cls, korean_name: str, english_name: str) -> None:
        """새로운 매핑 추가 (런타임에 동적 추가)"""
        if korean_name and english_name:
            cls.ARTIST_MAPPING[korean_name.strip()] = english_name.strip()
    
    @classmethod
    def get_all_names_for_artist(cls, artist_name: str) -> List[str]:
        """해당 아티스트의 모든 가능한 이름들 반환"""
        korean_name, english_name = cls.get_search_names(artist_name)
        
        names = [artist_name.strip()]  # 원본 이름
        
        if korean_name and korean_name not in names:
            names.append(korean_name)
            
        if english_name and english_name not in names:
            names.append(english_name)
            
        return [name for name in names if name.strip()]