import pandas as pd
import logging
import psycopg2
import os
from decimal import Decimal

def get_db_connection():
    """Get direct PostgreSQL connection"""
    return psycopg2.connect(os.environ.get("DATABASE_URL"))

def clean_text_for_sql(value):
    """Clean text value for SQL insertion"""
    if pd.isna(value) or value is None or str(value).lower() == 'nan':
        return None
    
    text = str(value).strip()
    
    # Remove NULL bytes and other problematic characters
    text = text.replace('\x00', '').replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ')
    text = text.replace('\t', ' ').replace('\\', '/').replace("'", "''")  # Escape single quotes
    
    # Limit length
    if len(text) > 500:
        text = text[:500]
    
    return text if text and text.lower() != 'nan' else None

def process_second_file_ultra_fast(df):
    """УЛЬТРА-ШВИДКА версія з обробкою по частинах і bulk операціями"""
    success_count = 0
    error_count = 0
    
    logging.info(f"🚀 ULTRA-FAST: Processing actualization file with {len(df)} rows")
    
    # Знайти ЄДRПОУ колонку (гнучкий пошук)
    edrpou_col = None
    for col_name in df.columns:
        if any(keyword in col_name.upper() for keyword in ['ЄДРПОУ', 'EDRPOU']):
            edrpou_col = col_name
            logging.info(f"Found EDRPOU column: '{col_name}'")
            break
    
    if not edrpou_col:
        logging.error("EDRPOU column not found in actualization file")
        return 0, len(df)
    
    # Отримати всі ЄДRПОУ та ID з бази одним запитом
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        logging.info("📊 Loading all companies from database...")
        cursor.execute("SELECT edrpou, id FROM companies")
        db_companies = {row[0]: row[1] for row in cursor.fetchall()}
        logging.info(f"Loaded {len(db_companies)} companies from database")
        
        # Фільтрувати файл актуалізації - залишити тільки існуючі компанії
        df[edrpou_col] = df[edrpou_col].astype(str)
        existing_mask = df[edrpou_col].isin(db_companies.keys())
        df_existing = df[existing_mask].copy()
        
        logging.info(f"🎯 Found {len(df_existing)} companies from actualization file that exist in database")
        logging.info(f"Skipping {len(df) - len(df_existing)} companies not found in database")
        
        if len(df_existing) == 0:
            logging.warning("No matching companies found - nothing to actualize")
            return 0, len(df)
        
        # СУПЕР-ШВИДКИЙ ПІДХІД: Обробити ВСІ дані одразу без циклів
        logging.info(f"⚡ SUPER-FAST: Processing all {len(df_existing)} companies at once")
        
        try:
            # Створити тимчасову таблицю для bulk update
            cursor.execute("""
                CREATE TEMPORARY TABLE temp_updates (
                    company_id INTEGER,
                    first_name TEXT,
                    middle_name TEXT,
                    last_name TEXT,
                    work_phone TEXT,
                    corporate_site TEXT,
                    work_email TEXT,
                    company_status TEXT,
                    director TEXT,
                    government_purchases NUMERIC,
                    tender_count INTEGER,
                    initials TEXT
                )
            """)
            
            # Підготувати ВСІ дані для одного bulk INSERT
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
                                if tender_count < 0:  # Перевірка на від'ємні
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
                    
                except Exception as e:
                    logging.error(f"Error processing row {index}: {str(e)}")
                    error_count += 1
                    continue
            
            # ОДИН ВЕЛИКИЙ BULK INSERT для всіх даних
            logging.info(f"💫 Bulk inserting {len(insert_data)} companies into temp table...")
            cursor.executemany("""
                INSERT INTO temp_updates VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, insert_data)
            
            # ОДИН UPDATE з JOIN для всіх компаній одразу
            logging.info(f"🚀 Performing single bulk UPDATE with JOIN...")
            cursor.execute("""
                UPDATE companies 
                SET 
                    first_name = COALESCE(t.first_name, companies.first_name),
                    middle_name = COALESCE(t.middle_name, companies.middle_name),
                    last_name = COALESCE(t.last_name, companies.last_name),
                    work_phone = COALESCE(t.work_phone, companies.work_phone),
                    corporate_site = COALESCE(t.corporate_site, companies.corporate_site),
                    work_email = COALESCE(t.work_email, companies.work_email),
                    company_status = COALESCE(t.company_status, companies.company_status),
                    director = COALESCE(t.director, companies.director),
                    government_purchases = COALESCE(t.government_purchases, companies.government_purchases),
                    tender_count = COALESCE(t.tender_count, companies.tender_count),
                    initials = COALESCE(t.initials, companies.initials)
                FROM temp_updates t
                WHERE companies.id = t.company_id
            """)
            
            affected_rows = cursor.rowcount
            success_count = affected_rows
            conn.commit()
            
            # Видалити тимчасову таблицю
            cursor.execute("DROP TABLE temp_updates")
            
            logging.info(f"⚡ SUPER-FAST COMPLETE: Updated {affected_rows} companies in one operation!")
            
        except Exception as e:
            conn.rollback()
            logging.error(f"Error in super-fast bulk update: {str(e)}")
            error_count = len(df_existing)
        
        logging.info(f"🎉 Ultra-fast actualization complete: {success_count} companies updated, {error_count} errors")
        
    except Exception as e:
        conn.rollback()
        logging.error(f"Database error during actualization: {str(e)}")
        return 0, len(df)
    finally:
        cursor.close()
        conn.close()
    
    return success_count, error_count