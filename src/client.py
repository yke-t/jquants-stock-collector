"""
J-Quants APIクライアント (V2対応)

V2 APIはAPIキー方式を使用。
ヘッダーに x-api-key でAPIキーを渡すだけで認証完了。

V2 エンドポイント:
- 銘柄一覧: /v2/equities/master
- 株価四本値: /v2/equities/bars/daily
- 財務情報: /v2/fins/summary
"""

import os
import requests
import time


class JQuantsClient:
    """J-Quants APIクライアント (V2対応)"""
    
    # V2 API Base URL
    BASE_URL = "https://api.jquants.com/v2"

    def __init__(self):
        """環境変数からAPIキーを取得してクライアントを初期化"""
        # V2ではAPIキーを使用（環境変数名は互換性のため両方サポート）
        self.api_key = os.getenv("JQUANTS_API_KEY") or os.getenv("JQUANTS_REFRESH_TOKEN")
        
        if not self.api_key:
            raise ValueError(
                "JQUANTS_API_KEY (or JQUANTS_REFRESH_TOKEN) environment variable is required."
            )
        
        # V2ではx-api-keyヘッダーを使用
        self.headers = {
            "x-api-key": self.api_key
        }

    def get(self, endpoint, params=None):
        """APIエンドポイントにGETリクエストを送る"""
        url = f"{self.BASE_URL}{endpoint}"
        
        max_retries = 10
        retry_count = 0
        
        while retry_count < max_retries:
            response = requests.get(url, headers=self.headers, params=params)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                raise Exception(f"Authentication failed (401): Check your API key")
            elif response.status_code == 403:
                raise Exception(f"Forbidden (403): {response.text}")
            elif response.status_code == 429:
                # レートリミット - 長めに待機
                wait_time = 5 * (retry_count + 1)  # 5, 10, 15, 20...秒
                print(f"[API] Rate limit, waiting {wait_time} seconds...")
                time.sleep(wait_time)
                retry_count += 1
                continue
            else:
                raise Exception(f"API Error {response.status_code}: {response.text}")
        
        raise Exception(f"Max retries exceeded for {endpoint}")
    
    # V2 API エンドポイント用のヘルパーメソッド
    def get_listed_info(self):
        """銘柄一覧を取得 (V2: /equities/master)"""
        return self.get("/equities/master")
    
    def get_daily_quotes(self, date=None, code=None):
        """日足データを取得 (V2: /equities/bars/daily)"""
        params = {}
        if date:
            params["date"] = date
        if code:
            params["code"] = code
        return self.get("/equities/bars/daily", params=params)
    
    def get_financial_summary(self, code=None):
        """財務情報を取得 (V2: /fins/summary)"""
        params = {}
        if code:
            params["code"] = code
        return self.get("/fins/summary", params=params)