"""
Модуль для злиття файлів Excel за принципом ВПР (VLOOKUP)
для системи Рейтинг.UA
"""
import pandas as pd
import logging
from datetime import datetime
from typing import Tuple, Dict, List

class ExcelFileMerger:
    """Клас для злиття двох Excel файлів за кодом ЄДРПОУ"""
    
    def __init__(self):
        self.main_file_data = None
        self.additional_file_data = None
        self.merged_data = None
        self.merge_stats = {
            'total_main': 0,
            'total_additional': 0,
            'matched': 0,
            'not_actualized': 0,
            'final_count': 0
        }
    
    def load_main_file(self, file_path: str) -> bool:
        """Завантажує основний файл (11 колонок)"""
        try:
            df = pd.read_excel(file_path)
            df = self._normalize_columns(df)
            
            # Перевірка наявності ЄДРПОУ
            if 'edrpou' not in df.columns:
                logging.error("Основний файл не містить колонку ЄДРПОУ")
                return False
            
            self.main_file_data = df
            self.merge_stats['total_main'] = len(df)
            
            logging.info(f"✅ Основний файл завантажено: {len(df)} записів")
            logging.info(f"📋 Колонки: {list(df.columns)}")
            
            return True
            
        except Exception as e:
            logging.error(f"Помилка завантаження основного файлу: {e}")
            return False
    
    def load_additional_file(self, file_path: str) -> bool:
        """Завантажує додатковий файл (17 додаткових колонок)"""
        try:
            df = pd.read_excel(file_path)
            df = self._normalize_columns(df)
            
            # Перевірка наявності ЄДРПОУ
            if 'edrpou' not in df.columns:
                logging.error("Додатковий файл не містить колонку ЄДРПОУ")
                return False
            
            self.additional_file_data = df
            self.merge_stats['total_additional'] = len(df)
            
            logging.info(f"✅ Додатковий файл завантажено: {len(df)} записів")
            logging.info(f"📋 Колонки: {list(df.columns)}")
            
            return True
            
        except Exception as e:
            logging.error(f"Помилка завантаження додаткового файлу: {e}")
            return False
    
    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Нормалізація назв колонок"""
        column_mapping = {
            'код єдрпоу': 'edrpou',
            'єдрпоу': 'edrpou',
            'edrpou': 'edrpou',
            'название компании': 'name',
            'название компании 2': 'name',
            'назва компанії': 'name',
            'компанія': 'name',
            'name': 'name',
            'квед': 'kved_code',
            'kved': 'kved_code',
            'основний вид діяльності (квед)': 'kved_description',
            'основний вид діяльності (квед) 2': 'kved_description',
            'основний вид діяльності': 'kved_description',
            'діяльність': 'kved_description',
            'персонал (2019 р.)': 'personnel_2019',
            'персонал (2019 р.) 2': 'personnel_2019',
            'персонал': 'personnel_2019',
            'personnel': 'personnel_2019',
            'область': 'region',
            'область 2': 'region',
            'регіон': 'region',
            'region': 'region',
            'телефон': 'phone',
            'phone': 'phone',
            'адреса реєстрації': 'address',
            'адреса реєстрації 2': 'address',
            'адреса': 'address',
            'address': 'address',
            'чистий дохід від реалізації продукції': 'revenue',
            'чистий дохід від реалізації продукції (товарів, робіт, послуг)': 'revenue',
            'чистий дохід (виручка) від реалізації продукції': 'revenue',
            'дохід від реалізації': 'revenue',
            'дохід': 'revenue',
            'виручка': 'revenue',
            'виручка, тис. грн (2019 р.)': 'revenue',
            'оборот': 'revenue',
            'revenue': 'revenue',
            'чистий фінансовий результат: прибуток': 'profit',
            'чистий фінансовий результат (прибуток)': 'profit',
            'фінансовий результат до оподаткування': 'profit',
            'прибуток (збиток) до оподаткування': 'profit',
            'чистий прибуток (збиток)': 'profit',
            'прибуток': 'profit',
            'чистий прибуток': 'profit',
            'profit': 'profit',
            'размер': 'size',
            'розмір': 'size',
            'size': 'size',
            
            # Додаткові колонки з другого файлу
            'рабочий телефон': 'work_phone',
            'корпоративный сайт': 'corporate_site',
            'рабочий e-mail': 'work_email',
            'стан компанії': 'company_status',
            'директор': 'director',
            'участь у держзакупівлях (на 01.04.2020)': 'government_purchases',
            'кількість тендерів': 'tender_count',
            'инициалы в падеже': 'initials',
            'имя': 'first_name',
            'отчество': 'middle_name',
            'фамилия': 'last_name'
        }
        
        # Нормалізація назв колонок
        df.columns = df.columns.astype(str).str.lower().str.strip()
        df.columns = [' '.join(col.split()) for col in df.columns]
        df = df.rename(columns=column_mapping)
        
        return df
    
    def merge_files(self, ranking_name: str = None) -> pd.DataFrame:
        """Об'єднує файли за принципом ВПР (VLOOKUP)"""
        if self.main_file_data is None or self.additional_file_data is None:
            logging.error("Спочатку завантажте обидва файли")
            return None
        
        try:
            # Об'єднання по ЄДРПОУ (LEFT JOIN - залишаємо всі записи з основного файлу)
            merged = self.main_file_data.merge(
                self.additional_file_data, 
                on='edrpou', 
                how='left', 
                suffixes=('', '_additional')
            )
            
            # Підрахунок статистики (перевірка будь-якої колонки з додаткового файлу)
            additional_columns = [col for col in merged.columns if col.endswith('_additional')]
            if additional_columns:
                matched_count = merged[additional_columns[0]].notna().sum()
                not_actualized_count = merged[additional_columns[0]].isna().sum()
            else:
                matched_count = 0
                not_actualized_count = len(merged)
            
            self.merge_stats['matched'] = matched_count
            self.merge_stats['not_actualized'] = not_actualized_count
            self.merge_stats['final_count'] = len(merged)
            
            # Додавання технічних колонок
            current_year = datetime.now().year
            
            merged['Источник'] = f"Україна {current_year}"
            merged['ТОП'] = 0  # Буде встановлено після ранжування
            merged['Загальна к-ть'] = len(merged)  # Буде оновлено після фільтрації
            # Знаходимо додаткові колонки для актуалізації
            additional_columns = [col for col in merged.columns if col.endswith('_additional')]
            if additional_columns:
                merged['Актуалізовано'] = merged[additional_columns[0]].notna().map({True: 'так', False: 'ні'})
                
                # Об'єднуємо назви, якщо є колонка name_additional
                if 'name_additional' in merged.columns:
                    merged['name'] = merged['name_additional'].fillna(merged['name'])
            else:
                merged['Актуалізовано'] = 'ні'
            
            # Видалення дублювання колонок
            columns_to_drop = [col for col in merged.columns if col.endswith('_additional')]
            merged = merged.drop(columns=columns_to_drop)
            
            self.merged_data = merged
            
            logging.info(f"✅ Файли об'єднано успішно!")
            logging.info(f"📊 Статистика злиття:")
            logging.info(f"   • Основний файл: {self.merge_stats['total_main']} записів")
            logging.info(f"   • Додатковий файл: {self.merge_stats['total_additional']} записів")
            logging.info(f"   • Знайдено співпадінь: {matched_count}")
            logging.info(f"   • Не актуалізовано: {not_actualized_count}")
            logging.info(f"   • Фінальний результат: {len(merged)} записів")
            
            return merged
            
        except Exception as e:
            logging.error(f"Помилка об'єднання файлів: {e}")
            return None
    
    def get_merged_data(self) -> pd.DataFrame:
        """Повертає об'єднані дані"""
        return self.merged_data
    
    def get_merge_statistics(self) -> Dict:
        """Повертає статистику об'єднання"""
        return self.merge_stats
    
    def update_ranking_columns(self, total_companies_in_ranking: int, total_companies_in_category: int):
        """Оновлює технічні колонки після створення рейтингу"""
        if self.merged_data is not None:
            # Оновлюємо ТОП (кількість компаній у рейтингу)
            self.merged_data.loc[self.merged_data.index < total_companies_in_ranking, 'ТОП'] = total_companies_in_ranking
            
            # Оновлюємо загальну кількість у категорії
            self.merged_data['Загальна к-ть'] = total_companies_in_category
    
    def preview_merged_data(self, rows: int = 10) -> pd.DataFrame:
        """Попередній перегляд об'єднаних даних"""
        if self.merged_data is None:
            return pd.DataFrame()
        
        # Показуємо важливі колонки для перевірки
        preview_columns = [
            'edrpou', 'name', 'kved_code', 'region', 'personnel_2019', 
            'revenue', 'profit', 'Источник', 'ТОП', 'Загальна к-ть', 'Актуалізовано'
        ]
        
        available_columns = [col for col in preview_columns if col in self.merged_data.columns]
        
        return self.merged_data[available_columns].head(rows)
    
    def export_to_csv(self, file_path: str) -> bool:
        """Експорт об'єднаних даних у CSV з усіма 33 колонками"""
        if self.merged_data is None:
            logging.error("Немає даних для експорту")
            return False
        
        try:
            # Перевіряємо, що у нас є всі необхідні колонки
            logging.info(f"📊 Експорт CSV з {len(self.merged_data.columns)} колонками")
            logging.info(f"📊 Кількість записів: {len(self.merged_data)}")
            
            self.merged_data.to_csv(file_path, index=False, encoding='utf-8')
            
            logging.info(f"✅ Дані експортовано у {file_path}")
            return True
            
        except Exception as e:
            logging.error(f"Помилка експорту CSV: {e}")
            return False