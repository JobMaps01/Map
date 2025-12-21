import pandas as pd
import re
import os
import json
import subprocess
import datetime
from collections import Counter
import sys
import urllib.parse
import numpy as np

# ==========================================
# 🔑 ВАШ КЛЮЧ
# ==========================================
API_KEY = "f8020690-59ad-43b3-974a-0e48a29c4a13"

# ==========================================
# 💰 НАСТРОЙКИ СТАВОК (Для почасовых)
# ==========================================
RATES_WS = {
    "Бариста": 270, "Дневной грузчик": 265, "Дневной продавец": 278,
    "Дневной сборщик": 265, "Кассир": 265, "Ночной грузчик": 274,
    "Ночной продавец": 287, "Охранник": 0, "Повар": 0, "Уборщица": 0,
}
RATES_DS = {
    "Грузчик-переборщик": 267, "Дневной грузчик": 267, "Кассир": 0,
    "Ночной грузчик": 400, "Ночной сборщик": 287, "Уборщица": 0,
}

project_dir = os.getcwd()
print(f"📂 Папка проекта: {project_dir}")

# --- 1. ЗАГРУЗКА ДАННЫХ ---
all_needs = []

# === ГЛАВНАЯ ФУНКЦИЯ "ЧИСТКИ" НАЗВАНИЙ ===
def standardize_role_name(name):
    clean = str(name).lower().strip()
    clean = " ".join(clean.split())

    if "построчно" in clean:
        if "сборщик" in clean:
            return "Сборщик (построчно)"
        return clean.capitalize()

    if "грузчик" in clean:
        return "Ночной грузчик" if "ноч" in clean else "Дневной грузчик"

    if "сборщик" in clean:
        return "Ночной сборщик" if "ноч" in clean else "Дневной сборщик"

    if "продавец" in clean:
        return "Ночной продавец" if "ноч" in clean else "Дневной продавец"

    if "кассир" in clean:
        return "Кассир"
    if "бариста" in clean:
        return "Бариста"
    if "убор" in clean or "клинер" in clean:
        return "Уборщица"
    if "повар" in clean:
        return "Повар"
    return clean.capitalize()

def clean_and_check(df, filename):
    df.columns = [str(c).strip() for c in df.columns]
    col_map = {
        "Роль": "Должность", "Кол-во сотрудников": "Количество сотрудников",
        "Кол-во": "Количество сотрудников", "Количество": "Количество сотрудников",
        "Дата": "Дата выхода"
    }
    df.rename(columns=col_map, inplace=True)
    if 'Должность' in df.columns:
        df['Должность'] = df['Должность'].apply(standardize_role_name)
    return df

# Ищем файлы (оптимизация: объединяем поиск Excel и CSV)
files = [f for f in os.listdir(project_dir) if ('потребность' in f.lower() and f.endswith('.xlsx')) or
         (f.endswith(".csv") and any(x in f for x in ["Сегодня", "Завтра", "ДС", "ВС-ГС"]))]

for filename in files:
    filepath = os.path.join(project_dir, filename)
    try:
        if filename.endswith('.xlsx'):
            xls = pd.ExcelFile(filepath)
            for sheet_name in xls.sheet_names:
                if any(x in sheet_name for x in ["Сегодня", "Завтра", "ДС", "ВС-ГС"]):
                    df = pd.read_excel(xls, sheet_name=sheet_name)
                    all_needs.append(clean_and_check(df, filename))
        elif filename.endswith('.csv'):
            df = pd.read_csv(filepath)
            all_needs.append(clean_and_check(df, filename))
    except Exception as e:
        print(f"⚠️ Ошибка при загрузке {filename}: {e}")

if not all_needs:
    print("🛑 ОШИБКА: Файлы не найдены.")
    sys.exit()

needs_df = pd.concat(all_needs, ignore_index=True)

# ==========================================
# 🔥 УДАЛЕНИЕ ДУБЛИКАТОВ
# ==========================================
print(f"📊 Всего строк до очистки: {len(needs_df)}")
dedup_cols = [col for col in ['ТТ', 'Должность', 'Дата выхода', 'Начало смены', 'Конец смены', 'Количество сотрудников']
              if col in needs_df.columns]
needs_df.drop_duplicates(subset=dedup_cols, keep='first', inplace=True)
print(f"✨ Строк после удаления дублей: {len(needs_df)}")

# Загрузка координат (оптимизация: поиск файла более эффективно)
coords_files = [f for f in os.listdir(project_dir) if ("coords" in f.lower() or "координаты" in f.lower()) and f.endswith(".csv")]
if not coords_files:
    print("🛑 ОШИБКА: Файл координат не найден.")
    sys.exit()
coords_filename = coords_files[0]  # Берем первый подходящий
coords_df = pd.read_csv(os.path.join(project_dir, coords_filename))
coords_df.columns = [c.strip() for c in coords_df.columns]

# --- 2. ОБРАБОТКА ---
print(f"✅ Данные загружены. Обработка {len(needs_df)} строк...")

def detect_store_type(tt_str):
    return "Darkstore" if "дс" in str(tt_str).lower() else "Whitestore"

needs_df['Тип_По_ТТ'] = needs_df['ТТ'].apply(detect_store_type)
needs_df['Дата_DT'] = pd.to_datetime(needs_df['Дата выхода'], dayfirst=True, errors='coerce')

# ==========================================
# 📅 ФИЛЬТР ПО ДАТЕ (ТОЛЬКО СЕГОДНЯ И БУДУЩЕЕ)
# ==========================================
today = pd.Timestamp.now().normalize()
rows_before = len(needs_df)
needs_df = needs_df[needs_df['Дата_DT'] >= today]
rows_after = len(needs_df)
print(f"📅 Фильтр по дате: удалено {rows_before - rows_after} старых вакансий.")
needs_df.sort_values(by=['ТТ', 'Должность', 'Дата_DT'], inplace=True)

# --- РАСЧЕТ ЧАСОВ И ЗАРПЛАТЫ (оптимизация: векторизованные операции где возможно) ---
def parse_time(time_str):
    try:
        h, m = map(int, str(time_str).split(':'))
        return h + m / 60
    except:
        return np.nan

needs_df['Start_Hour'] = needs_df['Начало смены'].apply(parse_time)
needs_df['End_Hour'] = needs_df['Конец смены'].apply(parse_time)
needs_df['Часы'] = np.where(needs_df['End_Hour'] < needs_df['Start_Hour'],
                            (24 - needs_df['Start_Hour']) + needs_df['End_Hour'],
                            needs_df['End_Hour'] - needs_df['Start_Hour'])
needs_df['Часы'] = needs_df['Часы'].fillna(0.0)

def get_pay_value(row):
    """Считает чистую сумму за смену (число)"""
    role = str(row['Должность'])
    hours = row['Часы']
    s_type = row['Тип_По_ТТ']

    if "построчно" in role.lower():
        return 0  # Сдельная, не считаем в сумму

    rate = RATES_DS.get(role, 0) if s_type == "Darkstore" else RATES_WS.get(role, 0)
    return int(hours * rate)

def get_pay_str(row):
    """Формирует строку для карточки"""
    val = row['Pay_Numeric']
    role = str(row['Должность'])
    s_type = row['Тип_По_ТТ']
    rate = RATES_DS.get(role, 0) if s_type == "Darkstore" else RATES_WS.get(role, 0)

    if "построчно" in role.lower():
        return "💰 Сдельная"
    if val > 0:
        return f"💰 {rate} ₽/ч (≈<b>{val}₽</b>)"
    return "💰 Уточняйте"

def get_role_icon(role):
    role = role.lower()
    if "грузчик" in role:
        return "📦"
    if "бариста" in role:
        return "☕"
    if "сборщик" in role:
        return "🎒"
    if "продавец" in role or "кассир" in role:
        return "🛒"
    return "🛒"

needs_df['Pay_Numeric'] = needs_df.apply(get_pay_value, axis=1)  # Число для расчетов
needs_df['Pay'] = needs_df.apply(get_pay_str, axis=1)  # Строка для карточки

# Создаем "Полное имя для фильтра" (Иконка + Название) сразу, чтобы посчитать мин/макс
needs_df['Icon'] = needs_df['Должность'].apply(get_role_icon)
needs_df['Filter_Name'] = needs_df['Icon'] + " " + needs_df['Должность']

# --- СБОР СТАТИСТИКИ ПО ЗАРПЛАТАМ ДЛЯ МЕНЮ ---
salary_stats = needs_df[needs_df['Pay_Numeric'] > 0].groupby('Filter_Name')['Pay_Numeric'].agg(['min', 'max']).to_dict('index')

# --- 3. MERGE С КООРДИНАТАМИ ---
def extract_tt(desc):
    m = re.search(r'Код ТТ:\s*([^\n\r"]+)', str(desc))
    return m.group(1).strip() if m else None

coords_df['JOIN_KEY'] = coords_df['Описание'].apply(extract_tt)
coords_clean = coords_df.drop_duplicates('JOIN_KEY')[['JOIN_KEY', 'Широта', 'Долгота', 'Адрес']]
full_data = needs_df.merge(coords_clean, left_on='ТТ', right_on='JOIN_KEY', how='left').dropna(subset=['Широта'])

# ==========================================
# 📍 ФИЛЬТР ПО МОСКВЕ И МО
# ==========================================
full_data = full_data[
    (full_data['Широта'] > 54.0) & (full_data['Широта'] < 57.5) &
    (full_data['Долгота'] > 35.0) & (full_data['Долгота'] < 41.0)
]

def make_card_html(row):
    d_str = row['Дата_DT'].strftime('%d.%m') if not pd.isna(row['Дата_DT']) else str(row['Дата выхода'])
    lat, lon = row['Широта'], row['Долгота']

    wa_text = (f"Здравствуйте! Хочу записаться на смену.\n"
               f"💼 Должность: {row['Должность']}\n"
               f"📍 Адрес: {row['Адрес']}\n"
               f"📅 Дата: {d_str}\n"
               f"🕒 Время: {row['Начало смены']} - {row['Конец смены']}")

    wa_encoded = urllib.parse.quote(wa_text)
    wa_link = f"https://wa.me/79152977432?text={wa_encoded}"
    w_nav = f"https://yandex.ru/maps/?rtext=~{lat},{lon}&rtt=mt"

    return (f"<div style='margin-bottom:12px; border-bottom:1px solid #eee; padding-bottom:8px; font-family:sans-serif;'>"
            f"📅 <b>{d_str}</b> | 👤 {row['Количество сотрудников']} чел.<br>"
            f"🕒 {row['Начало смены']} - {row['Конец смены']} | {row['Pay']}<br>"
            f"<div style='margin-top:8px; display:flex; flex-direction:column; gap:8px;'>"
            f"<a href='{wa_link}' target='_blank' style='background:#25D366; color:white; padding:10px; border-radius:6px; text-decoration:none; font-weight:bold; text-align:center;'>📝 Записаться через WhatsApp</a>"
            f"<a href='{w_nav}' target='_blank' style='background:#f0f0f0; color:black; border:1px solid #ccc; padding:8px; border-radius:6px; text-decoration:none; font-size:14px; text-align:center;'>📍 Построить маршрут</a>"
            f"<div style='display:flex; gap:5px; margin-top:5px;'>"
            f" <button onclick='openInfo()' style='flex:1; background:#007bff; color:white; border:none; padding:8px; border-radius:6px; cursor:pointer; font-weight:bold; font-size:12px;'>ℹ️ Инфо</button>"
            f" <button onclick='openWhatsAppWithGreeting()' style='flex:1; background:#128c7e; color:white; border:none; padding:8px; border-radius:6px; cursor:pointer; font-weight:bold; font-size:12px;'>📞 Менеджер</button>"
            f"</div>"
            f"</div></div>")

full_data['HTML_Card'] = full_data.apply(make_card_html, axis=1)

# Группируем для карты, добавляем Filter_Name чтобы знать тип
grouped = full_data.groupby(['ТТ', 'Должность', 'Filter_Name', 'Широта', 'Долгота', 'Адрес', 'Тип_По_ТТ'])['HTML_Card'].apply(''.join).reset_index()

# --- 4. СБОРКА WEB КАРТЫ ---
print("\n🚀 Генерируем обновленный интерфейс...")
features = []
filter_counts = Counter()

for idx, row in grouped.iterrows():
    role = row['Должность']
    filter_name = row['Filter_Name']
    store_type = "DS" if row['Тип_По_ТТ'] == "Darkstore" else "WS"

    filter_counts[filter_name] += 1

    features.append({
        "type": "Feature",
        "id": idx,
        "geometry": {"type": "Point", "coordinates": [row['Широта'], row['Долгота']]},
        "properties": {
            "balloonContentHeader": f"<b style='font-size:16px'>{role}</b> ({store_type})<br><span style='color:grey;font-size:13px'>{row['Адрес']}</span>",
            "balloonContentBody": f"<div style='max-height:300px; overflow-y:auto; font-size:14px'>{row['HTML_Card']}</div>",
            "clusterCaption": str(idx),
            "hintContent": role,
            "filterType": filter_name
        }
    })

json_data = json.dumps({"type": "FeatureCollection", "features": features}, ensure_ascii=False)

# === ГЕНЕРАЦИЯ КНОПОК С ЗАРПЛАТОЙ ===
buttons_html = ""
sorted_filters = sorted(filter_counts.items())

for name, count in sorted_filters:
    stats = salary_stats.get(name)

    salary_text = ""
    daily_pay_label = ""

    if stats:
        min_p = stats['min']
        max_p = stats['max']
        if min_p == max_p:
            salary_text = f"<span style='display:block; font-size:11px; color:#555; margin-top:2px;'>💰 {min_p} ₽/смена</span>"
        else:
            salary_text = f"<span style='display:block; font-size:11px; color:#555; margin-top:2px;'>💰 от {min_p} до {max_p} ₽</span>"

        daily_pay_label = "<span style='display:block; font-size:10px; color:#128c7e; font-weight:bold;'>⚡ оплата ежедневно</span>"
    elif "построчно" in name.lower():
        salary_text = "<span style='display:block; font-size:11px; color:#555; margin-top:2px;'>💰 Сдельная оплата</span>"
        daily_pay_label = "<span style='display:block; font-size:10px; color:#128c7e; font-weight:bold;'>⚡ оплата ежедневно</span>"

    buttons_html += f'''
    <button class="filter-btn" onclick="filterMap('{name}', this)">
        <div style="display:flex; flex-direction:column; align-items:flex-start;">
            <span class="btn-text">{name}</span>
            {salary_text}
            {daily_pay_label}
        </div>
        <span class="badge">{count}</span>
    </button>
    '''

total_points = len(grouped)
html_template = f"""<!DOCTYPE html>
<html>
<head>
    <title>Работа - Карта Смен</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <script src="https://api-maps.yandex.ru/2.1/?apikey={API_KEY}&lang=ru_RU"></script>
    <style>
        body, html {{ padding: 0; margin: 0; width: 100%; height: 100%; font-family: -apple-system, BlinkMacSystemFont, Roboto, Helvetica, Arial, sans-serif; }}
        #map {{ width: 100%; height: 100%; }}
        #menu-trigger {{
            position: absolute; top: 15px; left: 50%; transform: translateX(-50%); z-index: 1000;
            background: #fff; color: #333; padding: 10px 20px; border-radius: 30px;
            box-shadow: 0 4px 15px rgba(0,0,0,0.2); font-weight: bold; cursor: pointer; display: flex; align-items: center; gap: 8px; border: 1px solid #ddd;
        }}
        #controls {{
            position: absolute; top: 0; left: 0; z-index: 2000;
            background: #f4f4f6; width: 100%; height: 100%;
            display: flex; flex-direction: column;
            transition: transform 0.3s cubic-bezier(0.25, 0.46, 0.45, 0.94);
            transform: translateY(0);
        }}
        #controls.closed {{ transform: translateY(100%); }}
        @media (min-width: 768px) {{
            #controls {{ width: 350px; transform: translateX(0); border-right: 1px solid #ccc; }}
            #controls.closed {{ transform: translateX(-100%); }}
            #menu-trigger {{ display: none; }}
        }}
        .header {{ padding: 20px; background: #fff; box-shadow: 0 2px 5px rgba(0,0,0,0.05); }}
        .header-top {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px; }}
        .header h2 {{ margin: 0; font-size: 20px; }}
       
        .header-buttons {{ display: flex; gap: 8px; flex-wrap: wrap; }}
       
        .filters-list {{ padding: 15px; overflow-y: auto; flex: 1; }}
        .filter-btn {{
            width: 100%; display: flex; justify-content: space-between; align-items: center;
            padding: 12px 15px; margin-bottom: 10px; background: #fff; border: 1px solid #e0e0e0; border-radius: 12px;
            font-size: 15px; text-align: left; cursor: pointer; box-shadow: 0 2px 4px rgba(0,0,0,0.03); transition: all 0.2s;
        }}
        .filter-btn:active {{ transform: scale(0.98); background: #f0f0f0; }}
        .filter-btn.active {{ border: 2px solid #FFCC00; background: #fff9db; }}
        .badge {{ background: #eee; color: #555; padding: 4px 10px; border-radius: 20px; font-size: 13px; font-weight: bold; align-self: flex-start; margin-top: 5px; }}
        .close-btn {{ background: #e0e0e0; border: none; width: 36px; height: 36px; border-radius: 50%; font-size: 20px; cursor: pointer; display: flex; align-items: center; justify-content: center; }}
       
        .info-btn {{
            flex: 1;
            background: #007bff; color: white; border: none; padding: 10px 15px; border-radius: 8px;
            cursor: pointer; font-weight: bold; font-size: 13px; display: flex; align-items: center; justify-content: center; gap: 5px; text-decoration: none;
        }}
        .info-btn:hover {{ background: #0056b3; }}
        .manager-btn {{
            flex: 1;
            background: #25D366; color: white; border: none; padding: 10px 15px; border-radius: 8px;
            cursor: pointer; font-weight: bold; font-size: 13px; display: flex; align-items: center; justify-content: center; gap: 5px; text-decoration: none;
        }}
        .manager-btn:hover {{ background: #1ebc57; }}
        .modal-overlay {{
            display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%;
            background: rgba(0,0,0,0.5); z-index: 3000;
            justify-content: center; align-items: center;
        }}
        .modal-content {{
            background: white; padding: 25px; border-radius: 16px;
            max-width: 400px; width: 90%; position: relative;
            box-shadow: 0 10px 25px rgba(0,0,0,0.2);
            animation: fadeIn 0.3s;
            display: flex; flex-direction: column; gap: 10px;
        }}
        .modal-close {{
            position: absolute; top: 15px; right: 15px; font-size: 24px; cursor: pointer; color: #999;
        }}
        .step-box {{
            margin-bottom: 5px; padding-left: 15px; border-left: 4px solid #25D366;
            background: #f9f9f9; padding: 10px 10px 10px 15px; border-radius: 0 8px 8px 0;
        }}
        @keyframes fadeIn {{ from {{ opacity: 0; transform: translateY(10px); }} to {{ opacity: 1; transform: translateY(0); }} }}
    </style>
</head>
<body>
    <div id="infoModal" class="modal-overlay" onclick="closeModal(event)">
        <div class="modal-content">
            <span class="modal-close" onclick="document.getElementById('infoModal').style.display='none'">&times;</span>
            <h3 style="margin-top:0">🚀 Как устроиться?</h3>
           
            <div class="step-box">
                <b>1. 📄 Документы</b><br>
                🇷🇺 РФ: <b>Паспорт, Регистрация, ИНН</b>.<br>
                🌏 СНГ: <b>Полный пакет документов</b>.<br>
                + <b>Медкнижка</b>.
            </div>
            <div class="step-box">
                <b>2. 🤝 Знакомство</b><br>
                Согласуем выход на точку для знакомства с управляющим.
            </div>
            <div class="step-box">
                <b>3. ✅ Работа</b><br>
                Если всё устраивает — записывайтесь!
            </div>
           
            <button onclick="openWhatsAppWithGreeting()" class="manager-btn" style="width:100%; padding:12px; font-size:15px; margin-top:10px;">
                📞 Связаться с менеджером
            </button>
            <button onclick="document.getElementById('infoModal').style.display='none'" style="width:100%; padding:12px; background:#f0f0f0; color:#333; border:1px solid #ccc; border-radius:8px; font-weight:bold; font-size:15px; cursor:pointer;">
                Всё понятно
            </button>
        </div>
    </div>
    <div id="menu-trigger" onclick="openMenu()">🔍 ПОИСК РАБОТЫ</div>
   
    <div id="controls">
        <div class="header">
            <div class="header-top">
                <h2>Вакансии</h2>
                <button class="close-btn" onclick="closeMenu()">✕</button>
            </div>
           
            <div class="header-buttons">
                <button class="info-btn" onclick="openInfo()">ℹ️ Как устроиться</button>
                <button onclick="openWhatsAppWithGreeting()" class="manager-btn">📞 Связаться с менеджером</button>
            </div>
        </div>
       
        <div class="filters-list">
            <button class="filter-btn active" onclick="filterMap('all', this)">
                <span class="btn-text">🌍 ПОКАЗАТЬ ВСЕ</span>
                <span class="badge">{total_points}</span>
            </button>
            {buttons_html}
        </div>
    </div>
   
    <div id="map"></div>
   
    <script>
        let myMap, objectManager;
        const rawData = {json_data};
       
        function openWhatsAppWithGreeting() {{
            const date = new Date();
            const hour = date.getHours();
            let greeting = "Добрый день";
           
            if (hour >= 5 && hour < 12) {{
                greeting = "Доброе утро";
            }} else if (hour >= 12 && hour < 17) {{
                greeting = "Добрый день";
            }} else if (hour >= 17 && hour <= 23) {{
                greeting = "Добрый вечер";
            }} else {{
                greeting = "Доброй ночи";
            }}
           
            const text = `${{greeting}}! Хочу узнать подробности о работе во ВкусВилл`;
            const encoded = encodeURIComponent(text);
            const url = `https://wa.me/79152977432?text=${{encoded}}`;
           
            window.open(url, '_blank');
        }}
        ymaps.ready(init);
        function init () {{
            myMap = new ymaps.Map('map', {{
                center: [55.75, 37.62], zoom: 10,
                controls: ['zoomControl', 'geolocationControl']
            }});
           
            objectManager = new ymaps.ObjectManager({{
                clusterize: true, gridSize: 64, clusterDisableClickZoom: false
            }});
           
            objectManager.clusters.options.set('preset', 'islands#invertedYellowClusterIcons');
            myMap.geoObjects.add(objectManager);
            objectManager.add(rawData);
           
            const bounds = objectManager.getBounds();
            if (bounds) myMap.setBounds(bounds);
        }}
       
        function closeMenu() {{ document.getElementById('controls').classList.add('closed'); }}
        function openMenu() {{ document.getElementById('controls').classList.remove('closed'); }}
       
        function openInfo() {{ document.getElementById('infoModal').style.display = 'flex'; }}
        function closeModal(e) {{ if(e.target.id === 'infoModal') document.getElementById('infoModal').style.display='none'; }}
       
        function filterMap(category, btn) {{
            document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
           
            if (category === 'all') objectManager.setFilter('id >= 0');
            else objectManager.setFilter(object => object.properties.filterType === category);
           
            if (window.innerWidth < 768) closeMenu();
           
            setTimeout(() => {{
                const bounds = objectManager.getBounds();
                if (bounds) myMap.setBounds(bounds, {{checkZoomRange:true}});
            }}, 100);
        }}
    </script>
</body>
</html>"""

with open(os.path.join(project_dir, "index.html"), "w", encoding="utf-8") as f:
    f.write(html_template)
print("✅ Файл 'index.html' обновлен.")

# ==========================================
# 🚀 АВТОЗАГРУЗКА
# ==========================================
print("\n☁️ Начинаем загрузку на GitHub...")

def run_git_command(commands):
    try:
        result = subprocess.run(commands, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        return False, e.stderr

if run_git_command(["git", "--version"])[0]:
    run_git_command(["git", "add", "index.html"])
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    commit_success, commit_output = run_git_command(["git", "commit", "-m", f"Update salaries {timestamp}"])

    print("🔄 Синхронизация с сервером (git pull)...")
    pull_success, pull_output = run_git_command(["git", "pull", "--rebase", "--autostash", "-X", "ours"])
    if not pull_success:
        print(f"⚠️ Ошибка при пуле: {pull_output}")
    else:
        print(f"✅ Пул успешен: {pull_output}")

    print("⏳ Отправка на сервер...")
    push_success, push_output = run_git_command(["git", "push"])
    if push_success:
        print("🎉 УСПЕХ! Карта обновлена.")
        print("🔗 Ссылка: https://JobMaps01.github.io/Map/")
    else:
        if "nothing to commit" in commit_output:
            print("ℹ️ Изменений нет (карта уже актуальна).")
        else:
            print(f"⚠️ Ошибка при пуше: {push_output}")
else:
    print("⚠️ Git не найден.")