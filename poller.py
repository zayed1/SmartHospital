import csv
import time
import requests
import os
import json
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_IDS, STATE_JSON, SYNC_UPDATES_URL, SYNC_ONCALL_URL, SYNC_STAFF_URL

def load_csv_from_url(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        lines = response.text.strip().split('\n')
        return list(csv.DictReader(lines))
    except Exception as e:
        print(f"[ERROR] Failed to load CSV from {url}: {e}")
        return []

def load_state():
    try:
        with open(STATE_JSON, 'r') as f:
            return json.load(f)
    except:
        print(f"[DEBUG] state file not found, using fresh: {STATE_JSON}")
        return {"processed": [], "last_ts": "", "last_row": 0}

def save_state(state):
    with open(STATE_JSON, 'w') as f:
        json.dump(state, f)
    print(f"[DEBUG] state saved to {STATE_JSON} | processed={len(state['processed'])} last_ts={state['last_ts']} last_row={state['last_row']}")

def get_oncall_map(oncall_data):
    mapping = {}
    for row in oncall_data:
        dept = row['department'].strip()
        if row.get('authorized', 'yes').strip().lower() == 'yes':
            mapping[dept] = row['staff_id'].strip()
        else:
            print(f"[NO ONCALL] department={dept} has no authorized on-call")
    return mapping

def get_staff_map(staff_data):
    return {row['staff_id'].strip(): row for row in staff_data}

def get_chat_id_for_department(department):
    return TELEGRAM_CHAT_IDS.get(department)

def send_telegram_message(chat_id, text):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    r = requests.post(url, json=payload)
    return r.status_code == 200

def build_message(department, update):
    return f"""📢 تحديث جديد لقسم {department}

📌 {update['title']}

🗓️ التاريخ: {update['date']}
👨‍⚕️ الطبيب المناوب: {update['oncall_name']}
📞 التواصل: {update['oncall_mobile']}

#المستشفى_الذكي"""

def main():
    updates = load_csv_from_url(SYNC_UPDATES_URL)
    oncalls = load_csv_from_url(SYNC_ONCALL_URL)
    staff = load_csv_from_url(SYNC_STAFF_URL)

    oncall_map = get_oncall_map(oncalls)
    staff_map = get_staff_map(staff)
    state = load_state()

    sent_count = 0
    for row in updates:
        row_id = row['id']
        timestamp = row['timestamp']
        department = row['department'].strip()

        if row_id in state['processed']:
            continue

        staff_id = oncall_map.get(department)
        if not staff_id:
            continue

        oncall_info = staff_map.get(staff_id)
        if not oncall_info:
            continue

        row['oncall_name'] = oncall_info.get('name', 'غير معروف')
        row['oncall_mobile'] = oncall_info.get('mobile', 'غير متوفر')

        chat_id = get_chat_id_for_department(department)
        if not chat_id:
            print(f"[ERROR] No chat ID for department: {department}")
            continue

        message = build_message(department, row)
        if send_telegram_message(chat_id, message):
            print(f"[SENT] update_id={row_id} to chat_id={chat_id}")
            state['processed'].append(row_id)
            state['last_ts'] = timestamp
            state['last_row'] += 1
            sent_count += 1
        else:
            print(f"[FAILED] Failed to send message for update_id={row_id}")

    save_state(state)
    print(f"[DONE] processed={len(updates)} sent={sent_count} at {time.strftime('%Y-%m-%dT%H:%M:%S')}")

if __name__ == "__main__":
    main()
