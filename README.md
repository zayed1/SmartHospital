# المستشفى الخارق – MVP (Polling + WhatsApp)

هذا مشروع أولي (MVP) جاهز للتشغيل محليًا: يقرأ تحديثات من ملف CSV ويُرسل تنبيهات واتساب للمناوب الحالي في كل قسم.

> **المستوى:** بسيط جدًا – بدون تكامل مع HIS.  
> **القناة:** WhatsApp عبر Twilio API (يمكن استبداله بـ Meta Cloud لاحقًا).

---

## 🧱 المكونات
- `poller.py`: سكربت القراءة الدورية لملف `data/updates.csv`، ويُرسل الرسائل المناسبة.
- `config.py`: إعدادات عامة + تحميل المتغيرات من البيئة.
- `data/staff.csv`: قائمة الأرقام المصرّح لها (Binding).
- `data/oncall.csv`: من هو المناوب الحالي لكل قسم (يُحدثها المشرف مرة عند بداية الشفت).
- `data/updates.csv`: التحديثات (اسم المريض الثنائي، القسم، الحدث، الوقت).
- `data/state.json`: يتتبع آخر تحديث تم إرساله لمنع التكرار.
- `.env.example`: نموذج المتغيرات السرية (Twilio SID/Token/From).

---

## 🚀 طريقة التشغيل (محليًا)
1) **إنشاء بيئة بايثون** (اختياري):  
```bash
python -m venv .venv && source .venv/bin/activate  # على ويندوز: .venv\Scripts\activate
pip install pandas python-dotenv twilio
```

2) **نسخ ملف البيئة** وتعبئته:
```bash
cp .env.example .env
# افتح .env وعدّل القيم: TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_WHATSAPP_FROM
```

3) **حدّث الملفات التالية قبل التشغيل:**
- `data/staff.csv` : الأرقام المصرّح بها.
- `data/oncall.csv` : المناوب لكل قسم.
- `data/updates.csv` : صفوف التحديثات (يمكن أن يصدّرها المختبر/القبول).

4) **شغّل البولر:**
```bash
python poller.py --interval 60
```
- سيقرأ كل 60 ثانية ويرسل أي صف جديد.
- لتجربة فورية، أضف صفًا إلى `data/updates.csv` وشاهد الإرسال في الطرفية.

> ملاحظة: إذا تركت حقول Twilio فارغة، السكربت سيطبع الرسائل بدل إرسالها (وضع محاكاة).

---

## 🗂️ هيكل الملفات
```
mvp_whatsapp_bot/
  ├─ poller.py
  ├─ config.py
  ├─ README.md
  ├─ .env.example
  └─ data/
      ├─ staff.csv       # الاسم, القسم, الرقم, الدور, authorized
      ├─ oncall.csv      # القسم, الرقم
      ├─ updates.csv     # patient_name, department, event, timestamp
      └─ state.json      # { "last_ts": "..." , "last_row": 0 }
```

---

## 🛡️ الخصوصية
- الرسائل تحتوي **اسم ثنائي + القسم + الحدث** فقط.
- لا تُرسل إلا للأرقام الموجودة في `staff.csv` والمطابقة لـ `oncall.csv`.
- **لا بيانات طبية حساسة** داخل نص الرسالة.

---

## 🔄 تشغيل كـ خدمة/كرون
### كرون على لينكس:
افتح crontab:
```
crontab -e
```
أضف:
```
*/5 * * * * /usr/bin/env bash -lc 'cd /path/to/mvp_whatsapp_bot && source .venv/bin/activate && python poller.py --interval 0 --run-once'
```
> هذا يشغّل السكربت مرة كل 5 دقائق (بدون sleep داخل السكربت).

---

## 🔧 استبدال Twilio بـ Meta Cloud (لاحقًا)
- عدّل دالة الإرسال في `poller.py` لتستخدم `requests.post` إلى Meta Cloud Graph API.
- حافظ على نفس الواجهة: `send_whatsapp(to, text)`.

---

## 🧪 اختبار سريع
- تأكد أن `data/staff.csv` يحتوي رقمك مع authorized=1.
- ضع رقمك كـ on-call في `data/oncall.csv` لقسم (مثلاً "الباطنة").
- أضف صفًا في `data/updates.csv` بقسم "الباطنة" واسم "محمد أحمد" والحدث "نتيجة التحليل جاهزة" وتاريخ الآن.
- شغّل: `python poller.py --interval 0 --run-once`
- إذا إعداد Twilio صحيح → ستصلك رسالة واتساب. إذا لا → ستراها مطبوعة في الطرفية (محاكاة).

بالتوفيق!
