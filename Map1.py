import pandas as pd
import re
import os
import json
from collections import Counter

# ==========================================
# 🔑 ВАШ КЛЮЧ
# ==========================================
API_KEY = "f8020690-59ad-43b3-974a-0e48a29c4a13"

# ==========================================
# 💰 НАСТРОЙКИ СТАВОК
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
excel_files = [f for f in os.listdir(project_dir) if f.endswith('.xlsx') and 'потребность' in f.lower()]
if excel_files:
    try:
        xls = pd.ExcelFile(os.path.join(project_dir, excel_files[0]))
        for sheet_name in xls.sheet_names:
            if any(x in sheet_name for x in ["Сегодня", "Завтра", "ДС", "ВС-ГС"]):
                all_needs.append(pd.read_excel(xls, sheet_name=sheet_name))
    except Exception: pass

for filename in os.listdir(project_dir):
    if filename.endswith(".csv") and any(x in filename for x in ["Сегодня", "Завтра", "ДС", "ВС-ГС"]):
        try: all_needs.append(pd.read_csv(os.path.join(project_dir, filename)))
        except: pass

if not all_needs:
    print("🛑 ОШИБКА: Нет данных.")
    exit()

needs_df = pd.concat(all_needs, ignore_index=True)
coords_filename = next((f for f in os.listdir(project_dir) if ("coords" in f.lower() or "координаты" in f.lower()) and f.endswith(".csv")), None)
if not coords_filename:
    print("🛑 ОШИБКА: Нет координат.")
    exit()
coords_df = pd.read_csv(os.path.join(project_dir, coords_filename))
coords_df.columns = [c.strip() for c in coords_df.columns]

# --- 2. ОБРАБОТКА ---
def detect_store_type(tt_str):
    return "Darkstore" if "дс" in str(tt_str).lower() else "Whitestore"

needs_df['Тип_По_ТТ'] = needs_df['ТТ'].apply(detect_store_type)
needs_df['Дата_DT'] = pd.to_datetime(needs_df['Дата выхода'], dayfirst=True, errors='coerce')
needs_df.sort_values(by=['ТТ', 'Должность', 'Дата_DT'], inplace=True)

def get_hours(row):
    try:
        s, e = str(row['Начало смены']), str(row['Конец смены'])
        sh, sm = map(int, s.split(':')); eh, em = map(int, e.split(':'))
        start, end = sh + sm/60, eh + em/60
        return (24 - start) + end if end < start else end - start
    except: return 0.0
needs_df['Часы'] = needs_df.apply(get_hours, axis=1)

def get_pay(row):
    role, hours, s_type = str(row['Должность']), row['Часы'], row['Тип_По_ТТ']
    if "построчно" in role.lower(): return "💰 Сдельная"
    rate = RATES_DS.get(role, 0) if s_type == "Darkstore" else RATES_WS.get(role, 0)
    total = int(hours * rate)
    return f"💰 {rate} ₽/ч (≈<b>{total}₽</b>)" if rate > 0 else "💰 Уточняйте"
needs_df['Pay'] = needs_df.apply(get_pay, axis=1)

# --- 3. HTML КОНТЕНТ ---
def extract_tt(desc):
    m = re.search(r'Код ТТ:\s*([^\n\r"]+)', str(desc))
    return m.group(1).strip() if m else None

coords_df['JOIN_KEY'] = coords_df['Описание'].apply(extract_tt)
coords_clean = coords_df.drop_duplicates('JOIN_KEY')[['JOIN_KEY', 'Широта', 'Долгота', 'Адрес']]
full_data = needs_df.merge(coords_clean, left_on='ТТ', right_on='JOIN_KEY', how='left').dropna(subset=['Широта'])

def make_card_html(row):
    d_str = row['Дата_DT'].strftime('%d.%m') if not pd.isna(row['Дата_DT']) else str(row['Дата выхода'])
    lat, lon = row['Широта'], row['Долгота']
    w_nav = f"https://yandex.ru/maps/?rtext=~{lat},{lon}&rtt=mt"
    
    return (f"<div style='margin-bottom:12px; border-bottom:1px solid #eee; padding-bottom:8px; font-family:sans-serif;'>"
            f"📅 <b>{d_str}</b> | 👤 {row['Количество сотрудников']} чел.<br>"
            f"🕒 {row['Начало смены']} - {row['Конец смены']} | {row['Pay']}<br>"
            f"<div style='margin-top:8px; display:flex; gap:10px;'>"
            f"<a href='https://wa.me/79152977432' style='background:#25D366; color:white; padding:8px 16px; border-radius:6px; text-decoration:none; font-weight:bold; flex:1; text-align:center;'>WhatsApp</a>"
            f"<a href='{w_nav}' target='_blank' style='background:#fc0; color:black; padding:8px 16px; border-radius:6px; text-decoration:none; font-weight:bold; flex:1; text-align:center;'>📍 Маршрут</a>"
            f"</div></div>")

full_data['HTML_Card'] = full_data.apply(make_card_html, axis=1)
grouped = full_data.groupby(['ТТ', 'Должность', 'Широта', 'Долгота', 'Адрес', 'Тип_По_ТТ'])['HTML_Card'].apply(''.join).reset_index()

# --- 4. СБОРКА WEB КАРТЫ ---
print("\n🚀 Генерируем обновленный интерфейс...")

features = []
filter_counts = Counter()

for idx, row in grouped.iterrows():
    role = row['Должность']
    store_type = "DS" if row['Тип_По_ТТ'] == "Darkstore" else "WS"
    # Эмодзи для красоты в меню
    icon = "📦" if "грузчик" in role.lower() else "☕" if "бариста" in role.lower() else "🛒"
    
    filter_name = f"{icon} {store_type} {role}"
    filter_counts[filter_name] += 1
    
    features.append({
        "type": "Feature",
        "id": idx,
        "geometry": {"type": "Point", "coordinates": [row['Широта'], row['Долгота']]},
        "properties": {
            "balloonContentHeader": f"<b style='font-size:16px'>{filter_name}</b><br><span style='color:grey;font-size:13px'>{row['Адрес']}</span>",
            "balloonContentBody": f"<div style='max-height:250px; overflow-y:auto; font-size:14px'>{row['HTML_Card']}</div>",
            "clusterCaption": str(idx),
            "hintContent": role,
            "filterType": filter_name
        }
    })

json_data = json.dumps({"type": "FeatureCollection", "features": features}, ensure_ascii=False)

# Создаем красивые кнопки с количеством
buttons_html = ""
sorted_filters = sorted(filter_counts.items())
for name, count in sorted_filters:
    buttons_html += f'<button class="filter-btn" onclick="filterMap(\'{name}\', this)"><span class="btn-text">{name}</span> <span class="badge">{count}</span></button>'

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
        
        /* КНОПКА ОТКРЫТИЯ МЕНЮ (ПЛАВАЮЩАЯ) */
        #menu-trigger {{
            position: absolute; top: 15px; left: 50%; transform: translateX(-50%); z-index: 1000;
            background: #fff; color: #333; 
            padding: 10px 20px; border-radius: 30px;
            box-shadow: 0 4px 15px rgba(0,0,0,0.2);
            font-weight: bold; cursor: pointer; display: flex; align-items: center; gap: 8px;
            border: 1px solid #ddd;
        }}
        
        /* ШТОРКА МЕНЮ */
        #controls {{
            position: absolute; top: 0; left: 0; z-index: 2000;
            background: #f4f4f6; width: 100%; height: 100%;
            display: flex; flex-direction: column;
            transition: transform 0.3s cubic-bezier(0.25, 0.46, 0.45, 0.94);
            transform: translateY(0); /* По умолчанию ОТКРЫТО */
        }}
        
        #controls.closed {{ transform: translateY(100%); }}
        
        /* Для десктопа делаем боковую панель */
        @media (min-width: 768px) {{
            #controls {{ width: 350px; transform: translateX(0); border-right: 1px solid #ccc; }}
            #controls.closed {{ transform: translateX(-100%); }}
            #menu-trigger {{ left: 20px; transform: none; display: none; }} /* На десктопе кнопка не нужна, меню всегда видно */
        }}

        .header {{ 
            padding: 20px; background: #fff; 
            box-shadow: 0 2px 5px rgba(0,0,0,0.05);
            display: flex; justify-content: space-between; align-items: center;
        }}
        .header h2 {{ margin: 0; font-size: 20px; }}
        
        /* СПИСОК КНОПОК */
        .filters-list {{ 
            padding: 15px; overflow-y: auto; flex: 1; 
        }}
        
        .filter-btn {{
            width: 100%; display: flex; justify-content: space-between; align-items: center;
            padding: 15px; margin-bottom: 10px;
            background: #fff; border: 1px solid #e0e0e0; border-radius: 12px;
            font-size: 15px; text-align: left; cursor: pointer;
            box-shadow: 0 2px 4px rgba(0,0,0,0.03);
            transition: all 0.2s;
        }}
        
        .filter-btn:active {{ transform: scale(0.98); background: #f0f0f0; }}
        .filter-btn.active {{ border: 2px solid #FFCC00; background: #fff9db; }}
        
        .badge {{ 
            background: #eee; color: #555; 
            padding: 4px 10px; border-radius: 20px; font-size: 13px; font-weight: bold; 
        }}
        
        .close-btn {{ 
            background: #e0e0e0; border: none; width: 36px; height: 36px; 
            border-radius: 50%; font-size: 20px; cursor: pointer;
            display: flex; align-items: center; justify-content: center;
        }}

    </style>
</head>
<body>

    <div id="menu-trigger" onclick="openMenu()">
        🔍 ПОИСК РАБОТЫ
    </div>

    <div id="controls">
        <div class="header">
            <h2>Выберите работу</h2>
            <button class="close-btn" onclick="closeMenu()">✕</button>
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

        ymaps.ready(init);

        function init () {{
            myMap = new ymaps.Map('map', {{
                center: [55.75, 37.62], zoom: 10,
                controls: ['zoomControl', 'geolocationControl']
            }});

            objectManager = new ymaps.ObjectManager({{
                clusterize: true,
                gridSize: 64,
                clusterDisableClickZoom: false
            }});
            
            objectManager.clusters.options.set('preset', 'islands#invertedYellowClusterIcons');
            myMap.geoObjects.add(objectManager);
            objectManager.add(rawData);
            
            // Зум к точкам
            const bounds = objectManager.getBounds();
            if (bounds) myMap.setBounds(bounds);
        }}

        // Логика меню
        function closeMenu() {{
            document.getElementById('controls').classList.add('closed');
        }}
        
        function openMenu() {{
            document.getElementById('controls').classList.remove('closed');
        }}

        function filterMap(category, btn) {{
            // Подсветка активной кнопки
            document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');

            // Фильтрация
            if (category === 'all') {{
                objectManager.setFilter('id >= 0');
            }} else {{
                objectManager.setFilter(function (object) {{
                    return object.properties.filterType === category;
                }});
            }}
            
            // На мобильном закрываем меню после выбора, чтобы сразу видеть карту
            if (window.innerWidth < 768) {{
                closeMenu();
            }}
            
            // Пересчитываем зум
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

print("\n✅ ГОТОВО! Файл index.html обновлен.")
print("Загрузите его на GitHub, и по ссылке появится меню с категориями.")