import pandas as pd
import re
import os
from openpyxl import load_workbook
from openpyxl.styles import Alignment
# ==========================================
# === 💰 НАСТРОЙКИ СТАВОК (РЕДАКТИРОВАТЬ ЗДЕСЬ) ===
# ==========================================
# 1. Ставки для МАГАЗИНОВ (Вайтсторы) — стоимость часа
RATES_WS = {
    "Бариста": 270,
    "Дневной грузчик": 265,
    "Дневной продавец": 278,
    "Дневной сборщик": 265,
    "Кассир": 265,
    "Ночной грузчик": 274,
    "Ночной продавец": 287,
    "Охранник": 0,
    "Повар": 0,
    "Уборщица": 0,
    # Для должности "Сборщик (построчно)" автоматически будет стоять "Сдельная оплата"
}
# 2. Ставки для ДАРКСТОРОВ (Дарксторы) — стоимость часа
RATES_DS = {
    "Грузчик-переборщик": 267,
    "Дневной грузчик": 267,
    "Кассир": 0,
    "Ночной грузчик": 400,
    "Ночной сборщик": 287,
    "Уборщица": 0,
    # Для должности "Сборщик (построчно)" автоматически будет стоять "Сдельная оплата"
}
# ==========================================
project_dir = os.getcwd()
print(f"📂 Работаем в папке: {project_dir}")
print("-" * 30)
all_files = os.listdir(project_dir)
# --- 1. ЗАГРУЗКА ПОТРЕБНОСТИ ---
all_needs = []
found_source = False
# А) Пробуем Excel
excel_files = [f for f in all_files if f.endswith('.xlsx') and 'потребность' in f.lower()]
if excel_files:
    target_excel = excel_files[0]
    print(f"✅ Найден Excel файл: {target_excel}")
    try:
        xls = pd.ExcelFile(os.path.join(project_dir, target_excel))
        for sheet_name in xls.sheet_names:
            if any(x in sheet_name for x in ["Сегодня", "Завтра", "ДС", "ВС-ГС"]):
                df = pd.read_excel(xls, sheet_name=sheet_name)
                all_needs.append(df)
        if all_needs: found_source = True
    except Exception as e:
        print(f"❌ Ошибка Excel: {e}")
# Б) Пробуем CSV
if not found_source:
    patterns = ["Сегодня", "Завтра", "ДС", "ВС-ГС"]
    for filename in all_files:
        if not filename.endswith(".csv"): continue
        for pat in patterns:
            if pat in filename:
                try:
                    df = pd.read_csv(os.path.join(project_dir, filename))
                    all_needs.append(df)
                except: pass
if not all_needs:
    print("🛑 ОШИБКА: Нет данных о потребности. Проверьте файлы в папке.")
    exit()
needs_df = pd.concat(all_needs, ignore_index=True)
# Проверка обязательных колонок
required_cols = ['ТТ', 'Должность', 'Дата выхода', 'Начало смены', 'Конец смены', 'Количество сотрудников']
if not all(col in needs_df.columns for col in required_cols):
    print("🛑 ОШИБКА: Отсутствуют обязательные колонки в данных о потребности.")
    exit()
# --- 2. ЗАГРУЗКА КООРДИНАТ ---
coords_filename = next((f for f in all_files if ("coords" in f.lower() or "координаты" in f.lower()) and f.endswith(".csv")), None)
if coords_filename:
    try:
        coords_df = pd.read_csv(os.path.join(project_dir, coords_filename))
        coords_df.columns = [c.strip() for c in coords_df.columns]
    except Exception as e:
        print(f"❌ Ошибка координат: {e}")
        exit()
else:
    print("🛑 ОШИБКА: Файл координат не найден.")
    exit()
# --- 3. ОБРАБОТКА И РАСЧЕТ ОПЛАТЫ ---
print("\n⚙️ Расчет зарплат и формирование описаний...")
def detect_store_type_simple(tt_str):
    tt_str = str(tt_str).lower()
    if "дс" in tt_str or "даркстор" in tt_str:
        return "Darkstore"
    return "Whitestore"
needs_df['Тип_По_ТТ'] = needs_df['ТТ'].apply(detect_store_type_simple)
needs_df['Дата_DT'] = pd.to_datetime(needs_df['Дата выхода'], dayfirst=True, errors='coerce')
needs_df.sort_values(by=['ТТ', 'Должность', 'Дата_DT'], inplace=True)
def get_hours_float(row):
    try:
        s_str, e_str = str(row['Начало смены']), str(row['Конец смены'])
        if not re.match(r'^\d{1,2}:\d{2}$', s_str) or not re.match(r'^\d{1,2}:\d{2}$', e_str):
            return 0.0
        sh, sm = map(int, s_str.split(':'))
        eh, em = map(int, e_str.split(':'))
        start_h, end_h = sh + sm/60, eh + em/60
        return (24 - start_h) + end_h if end_h < start_h else end_h - start_h
    except: return 0.0
needs_df['Часы_Число'] = needs_df.apply(get_hours_float, axis=1)
def calculate_pay_str(row):
    role, hours, s_type = str(row['Должность']), row['Часы_Число'], row['Тип_По_ТТ']
    if "построчно" in role.lower():
        return "💰 <strong>Оплата:</strong> Сдельная (за пики)"
   
    rate = RATES_DS.get(role, 0) if s_type == "Darkstore" else RATES_WS.get(role, 0)
   
    if rate == 0:
        print(f"⚠️ Предупреждение: Ставка для '{role}' в '{s_type}' равна 0. Уточните ставки.")
   
    if rate > 0 and hours > 0:
        return f"💰 <strong>Оплата:</strong> {rate} ₽/час (≈ <strong>{int(hours * rate)} ₽</strong> за смену)"
    return f"💰 <strong>Оплата:</strong> {rate} ₽/час" if rate > 0 else "💰 <strong>Оплата:</strong> Уточняйте"
needs_df['Строка_Оплаты'] = needs_df.apply(calculate_pay_str, axis=1)
def create_html_item(row):
    date_str = row['Дата_DT'].strftime('%d.%m.%Y') if not pd.isna(row['Дата_DT']) else str(row['Дата выхода'])
    return (f"<li><br><strong>📅 Дата:</strong> {date_str}<br>"
            f"<strong>👤 Требуется:</strong> {row['Количество сотрудников']} чел.<br>"
            f"<strong>🕒 Смена:</strong> {row['Начало смены']} - {row['Конец смены']} (⏳ {row['Часы_Число']:g} ч.)<br>"
            f"{row['Строка_Оплаты']}<br>"
            f"<a href='https://wa.me/79152977432'>Записаться на смену в WhatsApp</a><br><br></li>")
needs_df['HTML_Item'] = needs_df.apply(create_html_item, axis=1)
grouped = needs_df.groupby(['ТТ', 'Должность'])['HTML_Item'].apply(''.join).reset_index()
def create_full_html_desc(row):
    return f"<p>🔹 <strong>{row['Должность']}</strong></p><p>👇 <strong>Открытые смены:</strong></p><ul>{row['HTML_Item']}</ul>"
grouped['Описание_Карты'] = grouped.apply(create_full_html_desc, axis=1)
# --- 4. СОХРАНЕНИЕ ---
def extract_tt_code(description):
    match = re.search(r'Код ТТ:\s*([^\n\r"]+)', str(description))
    return match.group(1).strip() if match else None
coords_df['JOIN_KEY'] = coords_df['Описание'].apply(extract_tt_code)
coords_unique = coords_df.drop_duplicates(subset=['JOIN_KEY'])[['JOIN_KEY', 'Широта', 'Долгота', 'Адрес']]
merged_df = grouped.merge(coords_unique, left_on='ТТ', right_on='JOIN_KEY', how='left')
merged_df['s_type'] = merged_df['ТТ'].apply(detect_store_type_simple)
# Проверка после merge
if merged_df['Широта'].isna().all():
    print("⚠️ Предупреждение: Нет совпадений по кодам ТТ в координатах.")
print("\n💾 Сохранение файлов по категориям...")
for (s_type, pos), group in merged_df.groupby(['s_type', 'Должность']):
    s_type_short = "DS" if s_type == "Darkstore" else "WS"
    pos_safe = re.sub(r'[\\/*?:"<>|]', "", str(pos)).strip()
   
    fname = f"Map_{s_type_short}_{pos_safe}.xlsx"
    export_df = group[['Широта', 'Долгота', 'Описание_Карты', 'Адрес']].dropna(subset=['Широта'])
    if export_df.empty: continue
    export_df.columns = ['Широта', 'Долгота', 'Описание', 'Подпись']
   
    with pd.ExcelWriter(os.path.join(project_dir, fname), engine='openpyxl') as writer:
        export_df.to_excel(writer, index=False, sheet_name='Data')
        ws = writer.sheets['Data']
        ws.column_dimensions['C'].width = 70
        for i, r in enumerate(ws.iter_rows(min_row=2), start=2):
            for c in r: c.alignment = Alignment(wrap_text=True, vertical='top')
            br_count = str(r[2].value).count('<br>') if r[2].value else 0
            ws.row_dimensions[i].height = (br_count + 5) * 15
print(f"\n🎉 Готово! Проверьте папку '{project_dir}'.")