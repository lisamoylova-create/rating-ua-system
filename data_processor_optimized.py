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

def process_second_file_optimized(df):
    """ОПТИМІЗОВАНА версія process_second_file з batch операціями"""
    success_count = 0
    error_count = 0
    
    logging.info(f"🚀 OPTIMIZED: Processing actualization file with {len(df)} rows")
    
    # Знайти ЄДРПОУ колонку (гнучкий пошук)
    edrpou_col = None
    for col_name in df.columns:
        if any(keyword in col_name.upper() for keyword in ['ЄДРПОУ', 'EDRPOU']):
            edrpou_col = col_name
            logging.info(f"Found EDRPOU column: '{col_name}'")
            break
    
    if not edrpou_col:
        logging.error("EDRPOU column not found in actualization file")
        return 0, len(df)
    
    # Отримати всі ЄДРПОУ та ID з бази одним запитом
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
        df_existing = df[existing_mask]
        
        logging.info(f"🎯 Found {len(df_existing)} companies from actualization file that exist in database")
        logging.info(f"Skipping {len(df) - len(df_existing)} companies not found in database")
        
        if len(df_existing) == 0:
            logging.warning("No matching companies found - nothing to actualize")
            return 0, len(df)
        
        # Обробити кожну існуючу компанію
        for index, row in df_existing.iterrows():
            try:
                edrpou = str(row[edrpou_col])
                company_id = db_companies[edrpou]
                
                # Підготувати дані для оновлення
                update_fields = []
                update_values = []
                
                # Ім'я
                first_name = clean_text_for_sql(
                    row.get('Ім\'я') or row.get('Имя') or row.get('First Name') or 
                    row.get('first_name') or row.get('Ім\'я директора')
                )
                if first_name:
                    update_fields.append('first_name = %s')
                    update_values.append(first_name)
                
                # По батькові
                middle_name = clean_text_for_sql(
                    row.get('По батькові') or row.get('Отчество') or row.get('Middle Name') or
                    row.get('middle_name') or row.get('По батькові директора')
                )
                if middle_name:
                    update_fields.append('middle_name = %s')
                    update_values.append(middle_name)
                
                # Прізвище
                last_name = clean_text_for_sql(
                    row.get('Прізвище') or row.get('Фамилия') or row.get('Last Name') or
                    row.get('last_name') or row.get('Прізвище директора')
                )
                if last_name:
                    update_fields.append('last_name = %s')
                    update_values.append(last_name)
                
                # Робочий телефон
                work_phone = clean_text_for_sql(
                    row.get('Робочий телефон') or row.get('Рабочий телефон') or 
                    row.get('Work Phone') or row.get('work_phone') or row.get('Тел. роб')
                )
                if work_phone:
                    update_fields.append('work_phone = %s')
                    update_values.append(work_phone)
                
                # Сайт
                corporate_site = clean_text_for_sql(
                    row.get('Корпоративний сайт') or row.get('Корпоративный сайт') or
                    row.get('Corporate Site') or row.get('corporate_site') or row.get('Сайт')
                )
                if corporate_site:
                    update_fields.append('corporate_site = %s')
                    update_values.append(corporate_site)
                
                # Email
                work_email = clean_text_for_sql(
                    row.get('Робочий e-mail') or row.get('Рабочий e-mail') or
                    row.get('Work Email') or row.get('work_email') or row.get('Email')
                )
                if work_email:
                    update_fields.append('work_email = %s')
                    update_values.append(work_email)
                
                # Статус компанії
                company_status = clean_text_for_sql(
                    row.get('Стан компанії') or row.get('Статус компании') or
                    row.get('Company Status') or row.get('company_status') or row.get('Статус')
                )
                if company_status:
                    update_fields.append('company_status = %s')
                    update_values.append(company_status)
                
                # Директор
                director = clean_text_for_sql(
                    row.get('Директор') or row.get('Director') or row.get('director') or
                    row.get('Керівник') or row.get('ПІБ директора')
                )
                if director:
                    update_fields.append('director = %s')
                    update_values.append(director)
                
                # Держзакупівлі (конвертувати в число: так=1, ні=0)
                government_purchases_raw = row.get('Участь у держзакупівлях (на 01.04.2020)') or row.get('Участие в госзакупках') or row.get('Government Purchases') or row.get('government_purchases') or row.get('Держзакупівлі')
                if government_purchases_raw and not pd.isna(government_purchases_raw):
                    gov_str = str(government_purchases_raw).strip().lower()
                    if gov_str in ['так', 'да', 'yes', '1', 'true']:
                        government_purchases = 1
                        update_fields.append('government_purchases = %s')
                        update_values.append(government_purchases)
                    elif gov_str in ['ні', 'нет', 'no', '0', 'false']:
                        government_purchases = 0
                        update_fields.append('government_purchases = %s')
                        update_values.append(government_purchases)
                
                # Кількість тендерів (тільки числові значення)
                tender_count_raw = row.get('Кількість тендерів') or row.get('Количество тендеров') or row.get('Tender Count')
                if tender_count_raw and not pd.isna(tender_count_raw):
                    try:
                        tender_str = str(tender_count_raw).strip().lower()
                        # Пропустити текстові значення типу "ні", "немає", "0"
                        if tender_str not in ['ні', 'нет', 'немає', 'нема', 'no', 'none', '']:
                            tender_count = int(float(tender_str))
                            if tender_count >= 0:  # Тільки позитивні числа
                                update_fields.append('tender_count = %s')
                                update_values.append(tender_count)
                    except (ValueError, TypeError):
                        # Якщо не вдається конвертувати - пропускаємо
                        pass
                
                # Ініціали
                initials = clean_text_for_sql(
                    row.get('Инициалы в падеже') or row.get('Ініціали в відмінку') or
                    row.get('Initials') or row.get('initials') or row.get('Ініціали')
                )
                if initials:
                    update_fields.append('initials = %s')
                    update_values.append(initials)
                
                # Якщо є що оновлювати
                if update_fields:
                    update_query = f"UPDATE companies SET {', '.join(update_fields)} WHERE id = %s"
                    update_values.append(company_id)
                    
                    cursor.execute(update_query, update_values)
                    success_count += 1
                    
                    if success_count % 100 == 0:
                        conn.commit()
                        logging.info(f"✅ Processed {success_count} companies")
                else:
                    error_count += 1
                    
            except Exception as e:
                logging.error(f"Error processing row {index}: {str(e)}")
                error_count += 1
                continue
        
        # Фінальний commit
        conn.commit()
        logging.info(f"🎉 Actualization complete: {success_count} companies updated, {error_count} errors")
        
    except Exception as e:
        conn.rollback()
        logging.error(f"Database error during actualization: {str(e)}")
        return 0, len(df)
    finally:
        cursor.close()
        conn.close()
    
    return success_count, error_count