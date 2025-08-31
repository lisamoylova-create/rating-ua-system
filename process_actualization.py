#!/usr/bin/env python3
"""
Зовнішній скрипт для обробки файлів актуалізації
Обробляє CSV файл актуалізації без timeout проблем веб-додатку
"""

import csv
import sys
import os
import psycopg2
from datetime import datetime
import pandas as pd

def get_db_connection():
    """Отримати з'єднання з базою даних"""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("ERROR: DATABASE_URL not found in environment variables")
        return None
    return psycopg2.connect(database_url)

def clean_text_for_sql(value):
    """Очистити текст для SQL"""
    if pd.isna(value) or value is None or str(value).lower() == 'nan':
        return None
    
    text = str(value).strip()
    
    # Видалити NULL bytes та інші проблематичні символи
    text = text.replace('\x00', '').replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ')
    text = text.replace('\t', ' ').replace('\\', '/').replace("'", "''")  # Escape single quotes
    
    # Обмежити довжину
    if len(text) > 500:
        text = text[:500]
    
    return text if text and text.lower() != 'nan' else None

def process_actualization_file(file_path):
    """Обробити CSV файл актуалізації і підготувати для завантаження в базу"""
    
    if not os.path.exists(file_path):
        print(f"ERROR: Файл {file_path} не знайдено")
        return False
    
    print(f"🚀 Початок обробки файлу актуалізації: {file_path}")
    print(f"⏰ Час початку: {datetime.now()}")
    
    success_count = 0
    error_count = 0
    
    # Отримати з'єднання з базою
    conn = get_db_connection()
    if not conn:
        print("ERROR: Не вдалося підключитися до бази даних")
        return False
    
    cursor = conn.cursor()
    
    try:
        # Завантажити всі ЄДRПОУ з бази для швидкої перевірки
        print("📊 Завантаження існуючих ЄДRПОУ з бази даних...")
        cursor.execute("SELECT edrpou, id FROM companies")
        db_companies = {row[0]: row[1] for row in cursor.fetchall()}
        print(f"✅ Завантажено {len(db_companies)} компаній з бази")
        
        # Читати CSV файл актуалізації
        print("📄 Читання CSV файлу актуалізації...")
        df = pd.read_csv(file_path, encoding='utf-8')
        print(f"📊 Прочитано {len(df)} рядків з файлу")
        
        # Знайти колонку ЄДRПОУ
        edrpou_col = None
        for col_name in df.columns:
            if any(keyword in col_name.upper() for keyword in ['ЄДРПОУ', 'EDRPOU']):
                edrpou_col = col_name
                print(f"✅ Знайдена колонка ЄДRПОУ: '{col_name}'")
                break
        
        if not edrpou_col:
            print("ERROR: Колонка ЄДRПОУ не знайдена в файлі актуалізації")
            return False
        
        # Фільтрувати тільки існуючі компанії
        df[edrpou_col] = df[edrpou_col].astype(str)
        existing_mask = df[edrpou_col].isin(db_companies.keys())
        df_existing = df[existing_mask].copy()
        
        print(f"🎯 Знайдено {len(df_existing)} компаній для актуалізації")
        print(f"⚠️ Пропущено {len(df) - len(df_existing)} компаній (не знайдено в базі)")
        
        if len(df_existing) == 0:
            print("WARNING: Жодна компанія не знайдена для актуалізації")
            return False
        
        # Підготовка до батчевої обробки (без тимчасових таблиць)
        print("🔧 Підготовка до батчевої актуалізації...")
        
        # Підготувати дані для батчевого оновлення
        print("📝 Підготовка даних для батчевого оновлення...")
        insert_data = []
        
        for index, row in df_existing.iterrows():
            try:
                edrpou = str(row[edrpou_col])
                company_id = db_companies[edrpou]
                
                # Обробити всі поля
                first_name = clean_text_for_sql(
                    row.get('Ім\'я') or row.get('Имя') or row.get('First Name') or 
                    row.get('first_name') or row.get('Ім\'я директора')
                )
                
                middle_name = clean_text_for_sql(
                    row.get('По батькові') or row.get('Отчество') or row.get('Middle Name') or
                    row.get('middle_name') or row.get('По батькові директора')
                )
                
                last_name = clean_text_for_sql(
                    row.get('Прізвище') or row.get('Фамилия') or row.get('Last Name') or
                    row.get('last_name') or row.get('Прізвище директора')
                )
                
                work_phone = clean_text_for_sql(
                    row.get('Робочий телефон') or row.get('Рабочий телефон') or 
                    row.get('Work Phone') or row.get('work_phone') or row.get('Тел. роб')
                )
                
                corporate_site = clean_text_for_sql(
                    row.get('Корпоративний сайт') or row.get('Корпоративный сайт') or
                    row.get('Corporate Site') or row.get('corporate_site') or row.get('Сайт')
                )
                
                work_email = clean_text_for_sql(
                    row.get('Робочий e-mail') or row.get('Рабочий e-mail') or
                    row.get('Work Email') or row.get('work_email') or row.get('Email')
                )
                
                company_status = clean_text_for_sql(
                    row.get('Стан компанії') or row.get('Статус компании') or
                    row.get('Company Status') or row.get('company_status') or row.get('Статус')
                )
                
                director = clean_text_for_sql(
                    row.get('Директор') or row.get('Director') or row.get('director') or
                    row.get('Керівник') or row.get('ПІБ директора')
                )
                
                # Держзакупівлі (конвертувати в число: так=1, ні=0)
                government_purchases = None
                government_purchases_raw = row.get('Участь у держзакупівлях (на 01.04.2020)') or row.get('Участие в госзакупках') or row.get('Government Purchases') or row.get('government_purchases') or row.get('Держзакупівлі')
                if government_purchases_raw and not pd.isna(government_purchases_raw):
                    gov_str = str(government_purchases_raw).strip().lower()
                    if gov_str in ['так', 'да', 'yes', '1', 'true']:
                        government_purchases = 1
                    elif gov_str in ['ні', 'нет', 'no', '0', 'false']:
                        government_purchases = 0
                
                # Кількість тендерів
                tender_count = None
                tender_count_raw = row.get('Кількість тендерів') or row.get('Количество тендеров') or row.get('Tender Count')
                if tender_count_raw and not pd.isna(tender_count_raw):
                    try:
                        tender_str = str(tender_count_raw).strip().lower()
                        if tender_str not in ['ні', 'нет', 'немає', 'нема', 'no', 'none', '']:
                            tender_count = int(float(tender_str))
                            if tender_count < 0:
                                tender_count = None
                    except (ValueError, TypeError):
                        tender_count = None
                
                # Ініціали
                initials = clean_text_for_sql(
                    row.get('Инициалы в падеже') or row.get('Ініціали в відмінку') or
                    row.get('Initials') or row.get('initials') or row.get('Ініціали')
                )
                
                # Додати дані для insert
                row_data = [
                    company_id,
                    first_name,
                    middle_name,
                    last_name,
                    work_phone,
                    corporate_site,
                    work_email,
                    company_status,
                    director,
                    government_purchases,
                    tender_count,
                    initials
                ]
                insert_data.append(row_data)
                success_count += 1
                
                # Прогрес кожні 100 записів
                if success_count % 100 == 0:
                    print(f"📊 Оброблено {success_count} записів...")
                
            except Exception as e:
                print(f"ERROR: Помилка обробки рядка {index}: {str(e)}")
                error_count += 1
                continue
        
        # Обробити дані батчами по 50 записів (замість bulk операції)
        print(f"🚀 Обробка {len(insert_data)} записів батчами по 50...")
        affected_rows = 0
        batch_size = 50
        
        for i in range(0, len(insert_data), batch_size):
            batch = insert_data[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(insert_data) + batch_size - 1) // batch_size
            
            print(f"📊 Обробка батчу {batch_num}/{total_batches} ({len(batch)} записів)...")
            
            # Обробити кожен запис в батчі окремим UPDATE
            for row_data in batch:
                company_id = row_data[0]
                update_query = """
                    UPDATE companies 
                    SET 
                        first_name = COALESCE(%s, first_name),
                        middle_name = COALESCE(%s, middle_name),
                        last_name = COALESCE(%s, last_name),
                        work_phone = COALESCE(%s, work_phone),
                        corporate_site = COALESCE(%s, corporate_site),
                        work_email = COALESCE(%s, work_email),
                        company_status = COALESCE(%s, company_status),
                        director = COALESCE(%s, director),
                        government_purchases = COALESCE(%s, government_purchases),
                        tender_count = COALESCE(%s, tender_count),
                        initials = COALESCE(%s, initials)
                    WHERE id = %s
                """
                
                update_values = row_data[1:] + [company_id]  # Всі поля + company_id для WHERE
                cursor.execute(update_query, update_values)
                
                if cursor.rowcount > 0:
                    affected_rows += cursor.rowcount
            
            # Зберегти батч в базу
            conn.commit()
            print(f"✅ Батч {batch_num}/{total_batches} завершено, оновлено записів: {affected_rows}")
        
        print(f"🎉 Всі батчі завершено! Загалом оновлено {affected_rows} компаній")
        
        # Створити результат файл з оновленими компаніями
        result_filename = file_path.replace('.csv', '_actualized.csv')
        with open(result_filename, 'w', newline='', encoding='utf-8') as result_file:
            writer = csv.writer(result_file)
            writer.writerow(['edrpou', 'status', 'updated_fields'])
            
            for index, row in df_existing.iterrows():
                edrpou = str(row[edrpou_col])
                writer.writerow([edrpou, 'updated', 'all_fields'])
        
        print(f"\n🎉 АКТУАЛІЗАЦІЯ ЗАВЕРШЕНА УСПІШНО!")
        print(f"✅ Успішно оновлено: {affected_rows} компаній")
        print(f"❌ Помилок: {error_count}")
        print(f"📄 Результат збережено в: {result_filename}")
        print(f"⏰ Час завершення: {datetime.now()}")
        
        return True
        
    except Exception as e:
        conn.rollback()
        print(f"CRITICAL ERROR: {str(e)}")
        return False
    
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python process_actualization.py <csv_file_path>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    if process_actualization_file(file_path):
        print("SUCCESS: Actualization completed successfully")
        sys.exit(0)
    else:
        print("FAILED: Actualization failed")
        sys.exit(1)