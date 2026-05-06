import os, requests
from dotenv import load_dotenv
load_dotenv()
key = os.getenv('API_KEY')
# Test a few dates to find one with data
for date in ['2026-03-30', '2026-03-31', '2026-04-01', '2026-04-10', '2026-04-28']:
    url = f'https://api.edinet-fsa.go.jp/api/v2/documents.json?date={date}&type=2&Subscription-Key={key}'
    r = requests.get(url, timeout=30)
    data = r.json()
    count = data.get('metadata', {}).get('resultset', {}).get('count', 0)
    print(f'{date}: {count} documents')
