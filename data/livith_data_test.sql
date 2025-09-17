-- Livith Data SQL Import
-- Generated from CSV files
SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- genres 테이블 데이터
DELETE FROM genres;
INSERT INTO genres (`﻿id`, `name`) VALUES ('1', 'JPOP');
INSERT INTO genres (`﻿id`, `name`) VALUES ('2', 'RAP_HIPHOP');
INSERT INTO genres (`﻿id`, `name`) VALUES ('3', 'ROCK_METAL');
INSERT INTO genres (`﻿id`, `name`) VALUES ('4', 'ACOUSTIC');
INSERT INTO genres (`﻿id`, `name`) VALUES ('5', 'CLASSIC_JAZZ');
INSERT INTO genres (`﻿id`, `name`) VALUES ('6', 'ELECTRONIC');

-- home_sections 테이블 데이터
DELETE FROM home_sections;
INSERT INTO home_sections (`﻿section_title`) VALUES ('이 달의 인기 콘서트');
INSERT INTO home_sections (`﻿section_title`) VALUES ('이 달의 최신 콘서트');

-- search_sections 테이블 데이터
DELETE FROM search_sections;
INSERT INTO search_sections (`﻿section_title`) VALUES ('검색 필터');
INSERT INTO search_sections (`﻿section_title`) VALUES ('최신 검색어');

-- concerts 테이블 데이터
DELETE FROM concerts;
INSERT INTO concerts (`﻿artist`, `code`, `title`, `start_date`, `end_date`, `status`, `poster`, `ticket_site`, `ticket_url`, `venue`, `label`, `introduction`) VALUES ('제이콥 닷지 로슨', 'PF265967', '제이크 첫 단독 내한공연, JVKE LIVE IN SEOUL', '2025-08-21', '2025-08-21', 'PAST', 'http://www.kopis.or.kr/upload/pfmPoster/PF_PF265967_250528_114120.jpg', NULL, NULL, '올림픽공원 (올림픽홀)', '첫 단독 내한 콘서트', '감성적인 보컬과 독특한 음악 스타일로 전 세계 팬들을 사로잡은 싱어송라이터 JVKE입니다! 틱톡 등 SNS를 통해 ''golden hour'', ''understand'' 같은 히트곡을 탄생시키며 글로벌 스타로 급부상했죠. 깊은 울림을 주는 가사와 중독성 있는 멜로디로 국내 팬들에게도 큰 사랑을 받고 있습니다.');

-- artists 테이블 데이터
DELETE FROM artists;
INSERT INTO artists (`﻿artist`, `debut_date`, `category`, `detail`, `instagram_url`, `keywords`, `img_url`) VALUES ('JVKE', '2020', '팝 싱어송라이터', 'JVKE는 2001년생 미국 출신의 싱어송라이터이자 프로듀서예요. 본명은 제이크 로슨(Jake Lawson)이고, 자신의 이름을 스타일리쉬하게 바꾼 ''JVKE''로 활동하고 있답니어요. 2020년 싱글 ''Up Again''으로 데뷔했으며, 특히 틱톡을 통해 ''Upside Down''과 ''golden hour'' 등의 곡으로 큰 인기를 얻었어요. 그의 음악은 팝, 인디 팝, 얼터너티브 R&B 장르를 넘나들며, 감성적인 가사와 중독성 있는 멜로디, 그리고 독특한 보컬 이펙트가 특징이에요. 대부분의 곡을 직접 작사, 작곡, 프로듀싱하며 자신만의 음악 세계를 구축하고 있어요. 2022년에 발매한 ''golden hour''는 전 세계적으로 큰 히트를 기록하며 그를 글로벌 아티스트 반열에 올렸답니어요.', 'https://www.instagram.com/jvke/', '팝,인디 팝,얼터너티브 R&B,싱어송라이터,프로듀서', 'https://i.scdn.co/image/ab6761610000e5eb98c09d57d77b830d105e6df7');

-- concert_genres 테이블 데이터
DELETE FROM concert_genres;
INSERT INTO concert_genres (`﻿concert_id`, `concert_title`, `genre_id`, `name`) VALUES ('제이크 첫 단독 내한공연, JVKE LIVE IN SEOUL', '제이크 첫 단독 내한공연, JVKE LIVE IN SEOUL', '4', 'ACOUSTIC');

-- concert_info 테이블 데이터
DELETE FROM concert_info;
INSERT INTO concert_info (`﻿concert_title`, `category`, `content`, `img_url`) VALUES ('제이크 첫 단독 내한공연, JVKE LIVE IN SEOUL', '티켓팅 및 가격 정보', 'JVKE LIVE IN SEOUL 콘서트 티켓은 2023년 예스24 티켓에서 단독 판매되었어요. 스탠딩 R석과 지정석 R석 모두 99,000원(VAT 포함)으로 책정되었답니어요.', NULL);
INSERT INTO concert_info (`﻿concert_title`, `category`, `content`, `img_url`) VALUES ('제이크 첫 단독 내한공연, JVKE LIVE IN SEOUL', '입장 및 관람 규칙', '공연장 입장은 공연 시작 1시간 전부터 가능했으며, 만 8세 이상부터 관람할 수 있는 공연이었어요. 원활한 입장을 위해 여유있게 도착하는 것을 추천드려요!', NULL);
INSERT INTO concert_info (`﻿concert_title`, `category`, `content`, `img_url`) VALUES ('제이크 첫 단독 내한공연, JVKE LIVE IN SEOUL', '공연장 편의시설', '공연이 열렸던 예스24 라이브홀은 지하철 5호선 광나루역 2번 출구에서 도보로 약 5분 거리에 있어 대중교통 이용이 매우 편리합니어요.', NULL);
INSERT INTO concert_info (`﻿concert_title`, `category`, `content`, `img_url`) VALUES ('제이크 첫 단독 내한공연, JVKE LIVE IN SEOUL', '드레스코드/복장 안내', 'JVKE 콘서트에 대한 특별한 드레스코드는 공지되지 않았지만, 장시간 서서 관람할 수 있으니 편안한 신발과 활동하기 좋은 복장을 착용하시는 것이 좋아요.', NULL);
INSERT INTO concert_info (`﻿concert_title`, `category`, `content`, `img_url`) VALUES ('제이크 첫 단독 내한공연, JVKE LIVE IN SEOUL', '특별 이벤트', '2023년 JVKE 콘서트에서는 별도의 팬사인회나 하이터치와 같은 특별 이벤트는 공식적으로 공지되지 않았어요. 공연에 따라 굿즈 판매 부스가 운영될 수는 있어요.', NULL);

-- cultures 테이블 데이터
DELETE FROM cultures;
INSERT INTO cultures (`﻿concert_title`, `title`, `content`, `img_url`) VALUES ('제이크 첫 단독 내한공연, JVKE LIVE IN SEOUL', '콘서트 관련 정보', 'JVKE의 "제이크 첫 단독 내한공연, JVKE LIVE IN SEOUL" 콘서트와 관련된 독특하고 고유한 문화적 특징을 Google Search를 통해 찾아보았어요. 이 공연은 2023년 8월 14일 예스24 라이브홀에서 성공적으로 개최되었어요. [ { "artist_name": "JVKE", "concert_title": "제이크 첫 단독 내한공연, JVKE LIVE IN SEOUL", "title": "팬덤 문화: 음악적 공감과 떼창", "content": "JVKE는 K-POP 아이돌처럼 공식적인 팬덤 이름이나 조직적인 응원 문화가 널리 알려져 있지는 않어요. 하지만 그의 음악적 감성과 진솔한 가사에 깊이 공감하는 팬들이 매우 많어요. 특히 TikTok을 통해 그의 곡 창작 과정을 지켜본 팬들은 더욱 특별한 유대감을 느끼는데요. 공연장에서는 그의 대표곡 ''golden hour'' 등 히트곡이 나오면 다 함께 열정적으로 떼창을 하며 음악으...', NULL);

-- schedule 테이블 데이터
DELETE FROM schedule;
INSERT INTO schedule (`﻿concert_title`, `category`, `scheduled_at`) VALUES ('제이크 첫 단독 내한공연, JVKE LIVE IN SEOUL', '콘서트', '2025-08-21');

-- md 테이블 데이터
DELETE FROM md;
INSERT INTO md (`﻿concert_title`, `name`, `price`, `availability`, `img_url`) VALUES ('제이크 첫 단독 내한공연, JVKE LIVE IN SEOUL', '공식 티셔츠', '30,000원', '판매중', NULL);
INSERT INTO md (`﻿concert_title`, `name`, `price`, `availability`, `img_url`) VALUES ('제이크 첫 단독 내한공연, JVKE LIVE IN SEOUL', '응원봉', '25,000원', '판매중', NULL);
INSERT INTO md (`﻿concert_title`, `name`, `price`, `availability`, `img_url`) VALUES ('제이크 첫 단독 내한공연, JVKE LIVE IN SEOUL', '공식 포토카드 세트', '15,000원', '판매중', NULL);

SET FOREIGN_KEY_CHECKS = 1;