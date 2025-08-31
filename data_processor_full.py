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

def clean_numeric_for_sql(value):
    """Clean numeric value for SQL insertion"""
    if pd.isna(value) or value is None or value == '':
        return None
    
    try:
        str_value = str(value).strip()
        str_value = str_value.replace(',', '').replace(' ', '').replace('₴', '').replace('грн', '')
        
        numeric_value = float(str_value)
        
        if numeric_value < 0 or numeric_value > 1000000000000:
            return None
            
        return numeric_value
    except (ValueError, TypeError):
        return None

def process_first_file(df):
    """Process first file with basic company data (11 columns)"""
    success_count = 0
    error_count = 0
    
    logging.info(f"Processing first file with {len(df)} rows")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        for index, row in df.iterrows():
            edrpou = None
            name = None
            try:
                # Basic required fields - map exact column names from the file
                edrpou_raw = row.get('Код ЄДРПОУ')
                # Skip rows with missing EDRPOU (NaN, None, empty, or zero)
                if pd.isna(edrpou_raw) or edrpou_raw is None or edrpou_raw == 0 or str(edrpou_raw).strip() == '':
                    logging.warning(f"Row {index}: Skipping row with empty EDRPOU")
                    error_count += 1
                    continue
                    
                edrpou = clean_text_for_sql(edrpou_raw)
                name = clean_text_for_sql(row.get('Название компании') or row.get('Назва') or row.get('Найменування'))
                
                if not edrpou or not name:
                    logging.error(f"Row {index}: Missing required fields - EDRPOU: '{edrpou}', Name: '{name}'")
                    error_count += 1
                    continue
                
                # Map exact column names from the file (removing extra whitespace)
                kved_code = clean_text_for_sql(row.get('КВЕД'))
                kved_description = clean_text_for_sql(row.get('Основний вид діяльності (КВЕД)'))
                personnel = clean_numeric_for_sql(row.get('Персонал (2019 р.)'))
                personnel = int(personnel) if personnel else None
                region = clean_text_for_sql(row.get('Область'))
                phone = clean_text_for_sql(row.get('Tелефон'))
                address = clean_text_for_sql(row.get('Адреса реєстрації'))
                # Use exact column names with trailing spaces
                revenue = clean_numeric_for_sql(row.get('Чистий дохід від реалізації продукції    '))
                profit = clean_numeric_for_sql(row.get('Чистий фінансовий результат: прибуток                                           '))
                size = clean_text_for_sql(row.get('Размер'))
                
                # Insert with all 33 columns, set additional ones to NULL initially
                cursor.execute("""
                    INSERT INTO companies 
                    (edrpou, name, kved_code, kved_description, personnel_2019, region_name, 
                     phone, address, revenue_2019, profit_2019, company_size_name,
                     first_name, middle_name, last_name, work_phone, corporate_site, work_email,
                     company_status, director, government_purchases, tender_count, initials,
                     source, actualized)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (edrpou) DO UPDATE SET
                        name = EXCLUDED.name,
                        kved_code = EXCLUDED.kved_code,
                        kved_description = EXCLUDED.kved_description,
                        personnel_2019 = EXCLUDED.personnel_2019,
                        region_name = EXCLUDED.region_name,
                        phone = EXCLUDED.phone,
                        address = EXCLUDED.address,
                        revenue_2019 = EXCLUDED.revenue_2019,
                        profit_2019 = EXCLUDED.profit_2019,
                        company_size_name = EXCLUDED.company_size_name,
                        updated_at = CURRENT_TIMESTAMP
                """, (edrpou, name, kved_code, kved_description, personnel, region, 
                      phone, address, revenue, profit, size,
                      None, None, None, None, None, None, None, None, None, None, None,  # 11 additional columns set to NULL
                      'основний', 'ні'))
                
                conn.commit()
                success_count += 1
                
                if success_count % 10 == 0:
                    logging.info(f"Processed {success_count} companies from first file")
                    
            except Exception as e:
                conn.rollback()
                logging.error(f"Error processing row {index}: {str(e)}")
                logging.error(f"Row data keys: {list(row.keys())}")
                logging.error(f"EDRPOU value: '{edrpou}', Name value: '{name}'")
                error_count += 1
                continue
    
    finally:
        cursor.close()
        conn.close()
    
    logging.info(f"First file processing complete: {success_count} success, {error_count} errors")
    return success_count, error_count

def process_second_file(df):
    """Process second file and UPDATE existing companies with additional data (17 columns)"""
    success_count = 0
    error_count = 0
    updated_count = 0
    
    logging.info(f"Processing second file with {len(df)} rows")
    
    # Check if this file has additional columns for actualization
    additional_columns = ['first_name', 'middle_name', 'last_name', 'work_phone', 
                         'corporate_site', 'work_email', 'company_status', 'director',
                         'government_purchases', 'tender_count', 'initials']
    
    has_additional_data = any(col in df.columns for col in additional_columns)
    
    # Process ONLY companies that have matching EDRPOU in the actualization file
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        for index, row in df.iterrows():
            try:
                # Extract EDRPOU from actualization file
                # Гнучкий пошук ЄДРПОУ колонки
                edrpou_raw = None
                for col_name in df.columns:
                    if any(keyword in col_name.upper() for keyword in ['ЄДРПОУ', 'EDRPOU']):
                        edrpou_raw = row.get(col_name)
                        break
                if pd.isna(edrpou_raw) or edrpou_raw is None or edrpou_raw == 0:
                    logging.warning(f"Row {index}: Skipping row with empty EDRPOU in actualization file")
                    error_count += 1
                    continue
                    
                edrpou = clean_text_for_sql(edrpou_raw)
                if not edrpou:
                    error_count += 1
                    continue
                
                # Check if company exists in our database
                cursor.execute("SELECT id FROM companies WHERE edrpou = %s", (edrpou,))
                existing_company = cursor.fetchone()
                
                if not existing_company:
                    logging.warning(f"Row {index}: Company with EDRPOU {edrpou} not found in database - skipping")
                    error_count += 1
                    continue
                
                # Extract additional data from actualization file with multiple column name variants
                update_fields = []
                update_values = []
                
                # Try different column name variants for each field
                first_name = clean_text_for_sql(
                    row.get('Ім\'я') or row.get('Имя') or row.get('First Name') or 
                    row.get('first_name') or row.get('Ім\'я директора')
                )
                if first_name:
                    update_fields.append('first_name = %s')
                    update_values.append(first_name)
                
                middle_name = clean_text_for_sql(
                    row.get('По батькові') or row.get('Отчество') or row.get('Middle Name') or
                    row.get('middle_name') or row.get('По батькові директора')
                )
                if middle_name:
                    update_fields.append('middle_name = %s')
                    update_values.append(middle_name)
                
                last_name = clean_text_for_sql(
                    row.get('Прізвище') or row.get('Фамилия') or row.get('Last Name') or
                    row.get('last_name') or row.get('Прізвище директора')
                )
                if last_name:
                    update_fields.append('last_name = %s')
                    update_values.append(last_name)
                
                work_phone = clean_text_for_sql(
                    row.get('Робочий телефон') or row.get('Рабочий телефон') or 
                    row.get('Work Phone') or row.get('work_phone') or row.get('Тел. роб')
                )
                if work_phone:
                    update_fields.append('work_phone = %s')
                    update_values.append(work_phone)
                
                corporate_site = clean_text_for_sql(
                    row.get('Корпоративний сайт') or row.get('Корпоративный сайт') or 
                    row.get('Website') or row.get('corporate_site') or row.get('Сайт')
                )
                if corporate_site:
                    update_fields.append('corporate_site = %s')
                    update_values.append(corporate_site)
                
                work_email = clean_text_for_sql(
                    row.get('Робочий e-mail') or row.get('Рабочий e-mail') or 
                    row.get('Email') or row.get('work_email') or row.get('E-mail')
                )
                if work_email:
                    update_fields.append('work_email = %s')
                    update_values.append(work_email)
                
                company_status = clean_text_for_sql(
                    row.get('Статус компанії') or row.get('Статус компании') or 
                    row.get('Status') or row.get('company_status')
                )
                if company_status:
                    update_fields.append('company_status = %s')
                    update_values.append(company_status)
                
                director = clean_text_for_sql(
                    row.get('Директор') or row.get('Керівник') or row.get('director')
                )
                if director:
                    update_fields.append('director = %s')
                    update_values.append(director)
                
                gov_purchases = clean_numeric_for_sql(
                    row.get('Держзакупівлі') or row.get('Госзакупки') or 
                    row.get('government_purchases') or row.get('Тендери сума')
                )
                if gov_purchases is not None:
                    update_fields.append('government_purchases = %s')
                    update_values.append(gov_purchases)
                
                tender_count = clean_numeric_for_sql(
                    row.get('Кількість тендерів') or row.get('Количество тендеров') or 
                    row.get('tender_count') or row.get('К-ть тендерів')
                )
                if tender_count is not None:
                    update_fields.append('tender_count = %s')
                    update_values.append(int(tender_count))
                
                initials = clean_text_for_sql(
                    row.get('Ініціали') or row.get('Инициалы') or row.get('initials')
                )
                if initials:
                    update_fields.append('initials = %s')
                    update_values.append(initials)
                
                # Mark as actualized regardless of whether additional data was found
                update_fields.append('actualized = %s')
                update_values.append('так')
                
                update_fields.append('source = %s')
                update_values.append('файл_2_актуалізація')
                
                update_fields.append('updated_at = CURRENT_TIMESTAMP')
                
                # Build and execute update query
                update_query = f"""
                UPDATE companies 
                SET {', '.join(update_fields)}
                WHERE edrpou = %s
                """
                update_values.append(edrpou)
                
                cursor.execute(update_query, update_values)
                success_count += 1
                
                if success_count % 10 == 0:
                    logging.info(f"Actualized {success_count} companies so far")
                    
            except Exception as e:
                logging.error(f"Error processing actualization row {index}: {str(e)}")
                error_count += 1
                continue
        
        conn.commit()
        logging.info(f"Actualization complete: {success_count} companies actualized, {error_count} errors")
        
    except Exception as e:
        conn.rollback()
        logging.error(f"Database error in actualization: {str(e)}")
        raise e
        
    finally:
        cursor.close()
        conn.close()
    
    return success_count, error_count
    
    # If has additional data, process normally
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        for index, row in df.iterrows():
            try:
                edrpou = clean_text_for_sql(row.get('Код ЄДРПОУ') or row.get('edrpou'))
                
                if not edrpou:
                    logging.warning(f"Row {index}: Missing EDRPOU in second file")
                    error_count += 1
                    continue
                
                # Check if company exists
                cursor.execute("SELECT id FROM companies WHERE edrpou = %s", (edrpou,))
                existing = cursor.fetchone()
                
                if not existing:
                    error_count += 1
                    continue
                
                # Extract additional data from second file
                first_name = clean_text_for_sql(row.get('first_name'))
                middle_name = clean_text_for_sql(row.get('middle_name')) 
                last_name = clean_text_for_sql(row.get('last_name'))
                work_phone = clean_text_for_sql(row.get('work_phone'))
                corporate_site = clean_text_for_sql(row.get('corporate_site'))
                work_email = clean_text_for_sql(row.get('work_email'))
                company_status = clean_text_for_sql(row.get('company_status'))
                director = clean_text_for_sql(row.get('director'))
                government_purchases = clean_numeric_for_sql(row.get('government_purchases'))
                tender_count = clean_numeric_for_sql(row.get('tender_count'))
                tender_count = int(tender_count) if tender_count else None
                initials = clean_text_for_sql(row.get('initials'))
                
                # Update existing company with additional data
                cursor.execute("""
                    UPDATE companies SET 
                        first_name = %s, middle_name = %s, last_name = %s,
                        work_phone = %s, corporate_site = %s, work_email = %s,
                        company_status = %s, director = %s, government_purchases = %s,
                        tender_count = %s, initials = %s,
                        actualized = 'так', updated_at = CURRENT_TIMESTAMP
                    WHERE edrpou = %s
                """, (first_name, middle_name, last_name, work_phone, corporate_site,
                      work_email, company_status, director, government_purchases, 
                      tender_count, initials, edrpou))
                
                conn.commit()
                updated_count += 1
                success_count += 1
                
                if success_count % 10 == 0:
                    logging.info(f"Updated {updated_count} companies from second file")
                    
            except Exception as e:
                conn.rollback()
                logging.error(f"Error updating row {index}: {str(e)}")
                error_count += 1
                continue
    
    finally:
        cursor.close()
        conn.close()
    
    logging.info(f"Second file processing complete: {updated_count} updated, {error_count} errors")
    return updated_count, error_count