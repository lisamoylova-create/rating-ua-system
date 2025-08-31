#!/usr/bin/env python3
"""
Окремий скрипт для обробки великих CSV файлів без проблем з UTF-8
Використовується поза Flask додатком для уникнення проблем з кодуванням
"""

import csv
import sys
import os
from datetime import datetime

def process_csv_file(file_path):
    """Обробити CSV файл і вивести статистику"""
    if not os.path.exists(file_path):
        print(f"Файл {file_path} не знайдено")
        return
    
    print(f"Початок обробки файлу: {file_path}")
    print(f"Час початку: {datetime.now()}")
    
    success_count = 0
    error_count = 0
    total_lines = 0
    
    valid_companies = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row_num, row in enumerate(reader, 1):
                total_lines += 1
                
                try:
                    # Перевірка ЄДРПОУ
                    edrpou = str(row.get('Код ЄДРПОУ', '') or row.get('ЄДРПОУ', '')).strip()
                    if not edrpou or edrpou == 'nan' or not edrpou.isdigit():
                        error_count += 1
                        print(f"Рядок {row_num}: Невалідний ЄДРПОУ '{edrpou}'")
                        continue
                    
                    # Збір даних компанії
                    company_data = {
                        'edrpou': edrpou,
                        'name': (row.get('Название компании', '') or '')[:500],
                        'kved_code': str(row.get('КВЕД', '') or '').strip()[:20],
                        'kved_description': (row.get('Основний вид діяльності (КВЕД)', '') or '')[:500],
                        'region_name': (row.get('Область', '') or '')[:100],
                        'phone': str(row.get('Tелефон', '') or '')[:50],
                        'address': (row.get('Адреса реєстрації', '') or '')[:500],
                        'company_size_name': (row.get('Размер', '') or '')[:50]
                    }
                    
                    # Парсинг числових полів
                    try:
                        personnel_str = str(row.get('Персонал (2019 р.)', '') or '')
                        if personnel_str and personnel_str.replace('.', '').replace(',', '').isdigit():
                            company_data['personnel_2019'] = int(float(personnel_str))
                        else:
                            company_data['personnel_2019'] = None
                    except:
                        company_data['personnel_2019'] = None
                    
                    try:
                        revenue_str = str(row.get('Чистий дохід від реалізації продукції    ', '') or '')
                        if revenue_str and revenue_str.replace(',', '.').replace(' ', ''):
                            company_data['revenue_2019'] = float(revenue_str.replace(',', '.'))
                        else:
                            company_data['revenue_2019'] = None
                    except:
                        company_data['revenue_2019'] = None
                    
                    try:
                        profit_str = str(row.get('Чистий фінансовий результат: прибуток                                           ', '') or '')
                        if profit_str and profit_str.replace(',', '.').replace(' ', ''):
                            company_data['profit_2019'] = float(profit_str.replace(',', '.'))
                        else:
                            company_data['profit_2019'] = None
                    except:
                        company_data['profit_2019'] = None
                    
                    valid_companies.append(company_data)
                    success_count += 1
                    
                    # Прогрес кожні 1000 записів
                    if row_num % 1000 == 0:
                        print(f"Оброблено {row_num} рядків, валідних: {success_count}, помилок: {error_count}")
                
                except Exception as e:
                    error_count += 1
                    print(f"Помилка в рядку {row_num}: {e}")
                    continue
        
        print(f"\n=== РЕЗУЛЬТАТ ОБРОБКИ ===")
        print(f"Всього рядків: {total_lines}")
        print(f"Успішно оброблено: {success_count}")
        print(f"Помилок: {error_count}")
        print(f"Час завершення: {datetime.now()}")
        
        # Збереження результатів у новий CSV
        if valid_companies:
            output_file = file_path.replace('.csv', '_processed.csv')
            with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
                fieldnames = ['edrpou', 'name', 'kved_code', 'kved_description', 'region_name', 
                             'phone', 'address', 'company_size_name', 'personnel_2019', 
                             'revenue_2019', 'profit_2019']
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()
                for company in valid_companies:
                    writer.writerow(company)
            
            print(f"Збережено оброблені дані у файл: {output_file}")
        
        # Показати приклади
        print(f"\n=== ПРИКЛАДИ ДАНИХ ===")
        for i, company in enumerate(valid_companies[:5]):
            print(f"{i+1}. {company['edrpou']}: {company['name'][:50]} ({company['kved_code']})")
    
    except Exception as e:
        print(f"Критична помилка: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Використання: python process_large_csv.py <шлях_до_файлу.csv>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    process_csv_file(file_path)