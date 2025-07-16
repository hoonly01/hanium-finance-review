import requests
import json
import pandas as pd
from datetime import datetime


def get_app_store_reviews_and_appname(app_id, country='kr', pages=10):
    """
    지정된 앱 ID와 국가 코드로 App Store 리뷰와 앱 이름을 가져옵니다.
    :return: (app_name, list of dict) 튜플 반환
    """
    all_reviews = []
    app_name = None
    for page in range(1, pages + 1):
        url = f"https://itunes.apple.com/{country}/rss/customerreviews/page={page}/id={app_id}/sortby=mostRecent/json"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            entries = data.get('feed', {}).get('entry')
            if not entries:
                print(f"{page} 페이지에서 더 이상 리뷰를 찾을 수 없어 중단합니다.")
                break
            if page == 1:
                # 첫 entry는 앱 정보, 여기서 앱 이름 추출
                app_name = entries[0].get('im:name', {}).get('label', f'app_{app_id}')
            for entry in entries[1:]:
                review = {
                    'author': entry.get('author', {}).get('name', {}).get('label', ''),
                    'rating': int(entry.get('im:rating', {}).get('label', 0)),
                    'title': entry.get('title', {}).get('label', ''),
                    'content': entry.get('content', {}).get('label', ''),
                    'updated_date': entry.get('updated', {}).get('label', '')
                }
                all_reviews.append(review)
            print(f"{page} 페이지의 리뷰를 성공적으로 가져왔습니다. (리뷰 수: {len(entries)-1})")
        except requests.exceptions.RequestException as e:
            print(f"HTTP 요청 중 에러 발생: {e}")
            break
        except json.JSONDecodeError:
            print("JSON 파싱 중 에러 발생. 응답이 올바른 JSON 형식이 아닙니다.")
            break
    return app_name, all_reviews


def read_app_ids(filename="app_ids.txt"):
    """
    app_ids.txt 파일에서 app_id만 추출하여 리스트로 반환
    주석(#) 및 빈 줄은 무시
    """
    app_ids = []
    with open(filename, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            # 주석이 붙은 경우 분리
            app_id = line.split('#')[0].strip()
            if app_id:
                app_ids.append(app_id)
    return app_ids


if __name__ == "__main__":
    # === 아래 변수만 수정하면 됩니다! ===
    COUNTRY = "kr"                # 예: 'kr', 'us', 'jp' 등
    PAGES_TO_CRAWL = 10           # 가져올 페이지 수 (1~10)
    # =============================

    app_ids = read_app_ids("app_ids.txt")
    if not app_ids:
        print("app_ids.txt 파일에 app_id가 없습니다. 파일을 확인하세요.")
    today_str = datetime.now().strftime('%Y%m%d')
    for app_id in app_ids:
        print(f"\n[앱 ID: {app_id}] 리뷰 크롤링 시작...")
        app_name, reviews_data = get_app_store_reviews_and_appname(app_id=app_id, country=COUNTRY, pages=PAGES_TO_CRAWL)
        if reviews_data:
            df = pd.DataFrame(reviews_data)
            # 파일명: 앱이름_날짜.csv (앱이름에 파일명 불가 문자가 있으면 _로 대체)
            safe_app_name = app_name.replace('/', '_').replace('\\', '_').replace(':', '_').replace('*', '_').replace('?', '_').replace('"', '_').replace('<', '_').replace('>', '_').replace('|', '_').replace(' ', '')
            output_filename = f'{safe_app_name}_{today_str}.csv'
            df.to_csv(output_filename, index=False, encoding='utf-8-sig')
            print(f"총 {len(reviews_data)}개의 리뷰를 수집하여 '{output_filename}' 파일로 저장했습니다.")
            print("파일 내용 미리보기:")
            print(df.head())
        else:
            print(f"앱 ID {app_id}의 리뷰 데이터를 가져오지 못했습니다.") 