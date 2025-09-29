
# 🏥 نظام التنبيهات والتقارير الذكية للمستشفى | Smart Hospital Alerts & Reports System

## 🇸🇦 النسخة العربية

هذا المشروع عبارة عن أداة خفيفة الوزن تعمل كـ **Poller** لقراءة التحديثات الطبية من ملف Google Sheets (منشور كـ CSV)، وتقوم بإرسال إشعارات مهيكلة إلى مجموعات Telegram الخاصة بالأقسام (مثل الطوارئ والباطنة). كما يحتوي المشروع على سكربت إضافي **Reporter** يقوم بإنشاء تقرير ملخص كل 6 ساعات عن التحديثات خلال آخر 24 ساعة.

### ⚙️ المميزات
- مزامنة تلقائية للتحديثات كل دقيقة (أو حسب التهيئة).
- دعم قوالب رسائل ديناميكية عبر ملف `templates.json`.
- إرسال التنبيهات مباشرة إلى مجموعات Telegram للأقسام.
- حفظ حالة آخر التحديثات المرسلة في ملف `state.json` على Volume.
- سكربت تقرير (reporter.py) يرسل ملخص كل 6 ساعات.
- دعم تعدد القنوات مستقبلًا (Telegram و SMS).

### 📂 هيكل المشروع
- `poller.py`: السكربت الرئيسي لمزامنة التحديثات وإرسال التنبيهات.
- `reporter.py`: سكربت إرسال التقارير الدورية.
- `config.py`: تحميل متغيرات البيئة وإعدادات المشروع.
- `data/updates.csv`: المصدر المحدث (يتم سحبه من Google Sheets).
- `data/templates.json`: قوالب الرسائل.
- `/data/state.json`: حالة آخر التحديثات (Volume).

### 🔑 المتغيرات البيئية الأساسية
- `DATA_DIR=data`
- `STATE_JSON=/data/state.json`
- `SYNC_UPDATES_URL=<رابط Google Sheets CSV>`
- `SYNC_ONCALL_URL=<رابط oncall.csv>`
- `SYNC_STAFF_URL=<رابط staff.csv>`
- `TELEGRAM_BOT_TOKEN=<توكن البوت>`
- `TELEGRAM_CHAT_IDS={"الطوارئ":"<id>","الباطنة":"<id>"}`

#### متغيرات إضافية للتقارير
- `REPORT_STATE=/data/report_state.json`
- `REPORT_HOURS=0,6,12,18`
- `REPORT_LOOKBACK_HOURS=24`

### 🚀 طريقة التشغيل
1. ضبط متغيرات البيئة في Railway (أو أي منصة استضافة).
2. وضع الملفات `poller.py`, `reporter.py`, `config.py`, و`data/templates.json`.
3. تعديل `Procfile` ليحتوي:
   ```
   web: python poller.py
   reporter: python reporter.py
   ```
4. نشر المشروع وتشغيله.


---

## 🇬🇧 English Version

This project is a lightweight **Poller** that continuously syncs medical updates from a published Google Sheets CSV and pushes structured notifications to Telegram groups mapped to hospital departments (e.g., ER, Internal Medicine). It also includes a **Reporter** script that generates 6-hourly summary reports covering the last 24 hours.

### ⚙️ Features
- Automatic sync every minute (configurable).
- Dynamic message templating via `templates.json`.
- Sends alerts directly to Telegram department groups.
- Persists processed state in `/data/state.json` (Volume).
- Periodic reporting (`reporter.py`) every 6 hours.
- Future support for multiple channels (Telegram, SMS).

### 📂 Project Structure
- `poller.py`: Main script for updates sync & alerts.
- `reporter.py`: Script for scheduled reports.
- `config.py`: Loads environment variables & config.
- `data/updates.csv`: Source file (synced from Google Sheets).
- `data/templates.json`: Message templates.
- `/data/state.json`: Processed state (Volume).

### 🔑 Required Environment Variables
- `DATA_DIR=data`
- `STATE_JSON=/data/state.json`
- `SYNC_UPDATES_URL=<Google Sheets CSV link>`
- `SYNC_ONCALL_URL=<oncall.csv link>`
- `SYNC_STAFF_URL=<staff.csv link>`
- `TELEGRAM_BOT_TOKEN=<Bot Token>`
- `TELEGRAM_CHAT_IDS={"ER":"<id>","Internal":"<id>"}`

#### Extra for Reports
- `REPORT_STATE=/data/report_state.json`
- `REPORT_HOURS=0,6,12,18`
- `REPORT_LOOKBACK_HOURS=24`

### 🚀 How to Run
1. Configure environment variables in Railway (or any hosting platform).
2. Include `poller.py`, `reporter.py`, `config.py`, and `data/templates.json`.
3. Update `Procfile` with:
   ```
   web: python poller.py
   reporter: python reporter.py
   ```
4. Deploy & run.
