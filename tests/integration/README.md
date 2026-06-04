# Integration Tests

このディレクトリには、外部API・実DB接続を必要とするテストを格納します。

## 実行方法

通常の `unittest discover tests` では実行されません。
明示的に指定して実行してください。

```powershell
# 個別実行
python tests/integration/test_real_signals.py
python tests/integration/test_analyzer.py
```

## 含まれるテスト

| ファイル | 依存 | 用途 |
|---|---|---|
| `test_real_signals.py` | Google Sheets, Google CSE API | 実シグナルでニュース分析をE2E検証 |
| `test_analyzer.py` | Google CSE API, yfinance | ニュースアナライザーの単体動作確認 |

## 注意

- これらのテストは `.env` の API キーや `secret_key.json` が必要です。
- CI/CD パイプラインでは実行しないでください。
