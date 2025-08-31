#!/usr/bin/env python3
"""
Швидке масове завантаження з використанням COPY FROM для максимальної швидкості
"""

import csv
import io
from app import app, db
import logging

def bulk_import_csv(file_path):
    """
    Найшвидший спосіб завантаження - використання PostgreSQL COPY FROM
    """
    with app.app_context():
        try:
            # Підготовка даних для bulk insert
            data_buffer = io.StringIO()
            
            # Читаємо CSV та конвертуємо у формат для COPY
            with open(file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                for row in reader:
                    edrpou = row.get('edrpou', '').strip()
                    if not edrpou:
                        continue
                    
                    # Підготовка рядка для COPY
                    values = [
                        edrpou,
                        row.get('name', '')[:500] or '',
                        row.get('kved_code', '')[:20] or '',
                        row.get('kved_description', '')[:500] or '',
                        row.get('region_name', '')[:100] or '',
                        row.get('phone', '')[:50] or '',
                        row.get('address', '')[:500] or '',
                        row.get('company_size_name', '')[:50] or '',
                        row.get('personnel_2019', '') or '0',
                        row.get('revenue_2019', '') or '0',
                        row.get('profit_2019', '') or '0',
                        'bulk_import',
                        'так'
                    ]
                    
                    # Екранування спеціальних символів для COPY
                    escaped_values = []
                    for val in values:
                        if val is None:
                            escaped_values.append('\\N')
                        else:
                            escaped_values.append(str(val).replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n'))
                    
                    data_buffer.write('\t'.join(escaped_values) + '\n')
            
            # Повертаємо до початку буфера
            data_buffer.seek(0)
            
            # Виконуємо COPY FROM для швидкого завантаження
            with db.session.connection().connection.cursor() as cursor:
                cursor.copy_from(
                    data_buffer,
                    'companies',
                    columns=[
                        'edrpou', 'name', 'kved_code', 'kved_description', 'region_name',
                        'phone', 'address', 'company_size_name', 'personnel_2019',
                        'revenue_2019', 'profit_2019', 'source', 'actualized'
                    ],
                    sep='\t'
                )
            
            db.session.commit()
            print(f"✓ Масове завантаження завершено успішно")
            
        except Exception as e:
            print(f"Помилка масового завантаження: {e}")
            db.session.rollback()

def prepare_temp_table_import(file_path):
    """
    Альтернативний швидкий спосіб через тимчасову таблицю
    """
    with app.app_context():
        try:
            # Створюємо тимчасову таблицю
            db.session.execute(db.text("""
                CREATE TEMP TABLE temp_companies (
                    edrpou VARCHAR(20),
                    name VARCHAR(500),
                    kved_code VARCHAR(20),
                    kved_description VARCHAR(500),
                    region_name VARCHAR(100),
                    phone VARCHAR(50),
                    address VARCHAR(500),
                    company_size_name VARCHAR(50),
                    personnel_2019 INTEGER,
                    revenue_2019 DECIMAL,
                    profit_2019 DECIMAL
                );
            """))
            
            # Завантажуємо дані у тимчасову таблицю через COPY
            with open(file_path, 'r') as f:
                with db.session.connection().connection.cursor() as cursor:
                    cursor.copy_expert(
                        "COPY temp_companies FROM STDIN WITH CSV HEADER DELIMITER ','",
                        f
                    )
            
            # Переносимо дані з тимчасової таблиці в основну з ON CONFLICT
            db.session.execute(db.text("""
                INSERT INTO companies (
                    edrpou, name, kved_code, kved_description, region_name,
                    phone, address, company_size_name, personnel_2019,
                    revenue_2019, profit_2019, source, actualized, created_at
                )
                SELECT 
                    edrpou, name, kved_code, kved_description, region_name,
                    phone, address, company_size_name, personnel_2019,
                    revenue_2019, profit_2019, 'bulk_temp', 'так', CURRENT_TIMESTAMP
                FROM temp_companies
                ON CONFLICT (edrpou) DO UPDATE SET
                    name = EXCLUDED.name,
                    kved_code = EXCLUDED.kved_code,
                    kved_description = EXCLUDED.kved_description,
                    region_name = EXCLUDED.region_name,
                    phone = EXCLUDED.phone,
                    address = EXCLUDED.address,
                    company_size_name = EXCLUDED.company_size_name,
                    personnel_2019 = EXCLUDED.personnel_2019,
                    revenue_2019 = EXCLUDED.revenue_2019,
                    profit_2019 = EXCLUDED.profit_2019,
                    updated_at = CURRENT_TIMESTAMP;
            """))
            
            db.session.commit()
            print("✓ Завантаження через тимчасову таблицю завершено")
            
        except Exception as e:
            print(f"Помилка завантаження через тимчасову таблицю: {e}")
            db.session.rollback()

if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        print(f"Швидке завантаження файлу: {file_path}")
        bulk_import_csv(file_path)
    else:
        print("Використання: python bulk_import_optimized.py <file_path>")