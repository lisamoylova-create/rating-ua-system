#!/usr/bin/env python3
"""
Скрипт для додавання оброблених CSV даних в базу PostgreSQL
"""

import csv
import os
import sys
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values

def get_db_connection():
    """Отримати з'єднання з базою даних"""
    try:
        conn = psycopg2.connect(os.environ.get('DATABASE_URL'))
        return conn
    except Exception as e:
        print(f"Помилка з'єднання з БД: {e}")
        return None

def process_csv_to_database(file_path):
    """Додати дані з обробленого CSV в базу PostgreSQL"""
    
    if not os.path.exists(file_path):
        print(f"Файл {file_path} не знайдено")
        return
    
    conn = get_db_connection()
    if not conn:
        return
    
    try:
        cur = conn.cursor()
        
        print(f"Початок завантаження в базу даних: {file_path}")
        print(f"Час початку: {datetime.now()}")
        
        # Підрахунок рядків
        with open(file_path, 'r', encoding='utf-8') as f:
            total_lines = sum(1 for _ in f) - 1  # без заголовка
        
        success_count = 0
        update_count = 0
        insert_count = 0
        error_count = 0
        
        with open(file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            batch_data = []
            
            for row_num, row in enumerate(reader, 1):
                try:
                    edrpou = row.get('edrpou', '').strip()
                    if not edrpou:
                        error_count += 1
                        continue
                    
                    # Перевірка чи існує компанія
                    cur.execute("SELECT id FROM companies WHERE edrpou = %s", (edrpou,))
                    existing = cur.fetchone()
                    
                    # Підготовка даних
                    company_data = {
                        'edrpou': edrpou,
                        'name': row.get('name', '')[:500] if row.get('name') else None,
                        'kved_code': row.get('kved_code', '')[:20] if row.get('kved_code') else None,
                        'kved_description': row.get('kved_description', '')[:500] if row.get('kved_description') else None,
                        'region_name': row.get('region_name', '')[:100] if row.get('region_name') else None,
                        'phone': row.get('phone', '')[:50] if row.get('phone') else None,
                        'address': row.get('address', '')[:500] if row.get('address') else None,
                        'company_size_name': row.get('company_size_name', '')[:50] if row.get('company_size_name') else None,
                        'personnel_2019': int(float(row.get('personnel_2019', 0))) if row.get('personnel_2019') else None,
                        'revenue_2019': float(row.get('revenue_2019', 0)) if row.get('revenue_2019') else None,
                        'profit_2019': float(row.get('profit_2019', 0)) if row.get('profit_2019') else None
                    }
                    
                    if existing:
                        # Оновлення існуючої компанії
                        update_sql = """
                        UPDATE companies SET 
                            name = COALESCE(%s, name),
                            kved_code = COALESCE(%s, kved_code),
                            kved_description = COALESCE(%s, kved_description),
                            region_name = COALESCE(%s, region_name),
                            phone = COALESCE(%s, phone),
                            address = COALESCE(%s, address),
                            company_size_name = COALESCE(%s, company_size_name),
                            personnel_2019 = COALESCE(%s, personnel_2019),
                            revenue_2019 = COALESCE(%s, revenue_2019),
                            profit_2019 = COALESCE(%s, profit_2019),
                            updated_at = CURRENT_TIMESTAMP
                        WHERE edrpou = %s
                        """
                        cur.execute(update_sql, (
                            company_data['name'],
                            company_data['kved_code'],
                            company_data['kved_description'],
                            company_data['region_name'],
                            company_data['phone'],
                            company_data['address'],
                            company_data['company_size_name'],
                            company_data['personnel_2019'],
                            company_data['revenue_2019'],
                            company_data['profit_2019'],
                            edrpou
                        ))
                        update_count += 1
                    else:
                        # Вставка нової компанії
                        insert_sql = """
                        INSERT INTO companies (
                            edrpou, name, kved_code, kved_description, region_name,
                            phone, address, company_size_name, personnel_2019,
                            revenue_2019, profit_2019, source, actualized, created_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        """
                        cur.execute(insert_sql, (
                            company_data['edrpou'],
                            company_data['name'],
                            company_data['kved_code'],
                            company_data['kved_description'],
                            company_data['region_name'],
                            company_data['phone'],
                            company_data['address'],
                            company_data['company_size_name'],
                            company_data['personnel_2019'],
                            company_data['revenue_2019'],
                            company_data['profit_2019'],
                            'імпорт',
                            'так'
                        ))
                        insert_count += 1
                    
                    success_count += 1
                    
                    # Commit кожні 100 записів
                    if row_num % 100 == 0:
                        conn.commit()
                        print(f"Оброблено {row_num}/{total_lines}: оновлено {update_count}, додано {insert_count}, помилок {error_count}")
                
                except Exception as e:
                    error_count += 1
                    print(f"Помилка в рядку {row_num}: {e}")
                    continue
        
        # Фінальний commit
        conn.commit()
        
        print(f"\n=== РЕЗУЛЬТАТ ЗАВАНТАЖЕННЯ В БД ===")
        print(f"Всього рядків: {total_lines}")
        print(f"Успішно оброблено: {success_count}")
        print(f"Оновлено існуючих: {update_count}")
        print(f"Додано нових: {insert_count}")
        print(f"Помилок: {error_count}")
        print(f"Час завершення: {datetime.now()}")
        
        # Перевірка кількості компаній в БД
        cur.execute("SELECT COUNT(*) FROM companies")
        total_companies = cur.fetchone()[0]
        print(f"Загальна кількість компаній в БД: {total_companies}")
        
    except Exception as e:
        print(f"Критична помилка: {e}")
        conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Використання: python create_companies_from_processed.py <шлях_до_обробленого_файлу.csv>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    process_csv_to_database(file_path)