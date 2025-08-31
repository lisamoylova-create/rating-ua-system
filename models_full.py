from app import db
from datetime import datetime
from flask_login import UserMixin

class User(UserMixin, db.Model):
    __tablename__ = 'users'
    
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.Text, unique=True, nullable=False)
    email = db.Column(db.Text, unique=True, nullable=False)
    password_hash = db.Column(db.Text, nullable=False)
    role = db.Column(db.Text, default='viewer')
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def has_permission(self, permission):
        permissions = {
            'admin': ['view', 'edit', 'upload', 'export', 'actualize', 'manage_users'],
            'manager': ['view', 'edit', 'export', 'ranking'],
            'guest': ['view']
        }
        return permission in permissions.get(self.role, [])

class Company(db.Model):
    __tablename__ = 'companies'
    
    # Primary key and identifier
    id = db.Column(db.Integer, primary_key=True)
    edrpou = db.Column(db.Text, unique=True, nullable=False, index=True)
    
    # Basic company info (11 основних колонок з першого файлу)
    name = db.Column(db.Text, nullable=False)
    kved_code = db.Column(db.Text)
    kved_description = db.Column(db.Text)
    personnel_2019 = db.Column(db.Integer)
    region_name = db.Column(db.Text)
    phone = db.Column(db.Text)  # тілефон з першого файлу
    address = db.Column(db.Text)
    revenue_2019 = db.Column(db.Numeric(15,2))
    profit_2019 = db.Column(db.Numeric(15,2))
    company_size_name = db.Column(db.Text)
    
    # Additional fields from second file (17 додаткових колонок)
    first_name = db.Column(db.Text)  # Ім'я керівника
    middle_name = db.Column(db.Text)  # По батькові
    last_name = db.Column(db.Text)  # Прізвище
    work_phone = db.Column(db.Text)  # Робочий телефон
    corporate_site = db.Column(db.Text)  # Корпоративний сайт
    work_email = db.Column(db.Text)  # Робоча пошта
    company_status = db.Column(db.Text)  # Статус компанії
    director = db.Column(db.Text)  # Директор (повне ім'я)
    government_purchases = db.Column(db.Numeric(15,2))  # Держзакупівлі
    tender_count = db.Column(db.Integer)  # Кількість тендерів
    initials = db.Column(db.Text)  # Ініціали
    
    # Technical fields (5 технічних колонок)
    source = db.Column(db.Text, default='основний')  # Джерело завантаження
    actualized = db.Column(db.Text, default='ні')  # Чи актуалізовано з другого файлу
    ranking = db.Column(db.Integer)  # Позиція в рейтингу
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

# Selection models for the 3-stage process
class SelectionBase(db.Model):
    __tablename__ = 'selection_bases'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text, nullable=False)
    min_employees = db.Column(db.Integer)
    min_revenue = db.Column(db.Numeric(15,2))
    min_profit = db.Column(db.Numeric(15,2))
    companies_count = db.Column(db.Integer, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    is_active = db.Column(db.Boolean, default=True)

class SelectionCompany(db.Model):
    __tablename__ = 'selection_companies'
    
    id = db.Column(db.Integer, primary_key=True)
    selection_base_id = db.Column(db.Integer, nullable=False)
    company_id = db.Column(db.Integer, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class Ranking(db.Model):
    __tablename__ = 'rankings'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text, nullable=False)
    selection_base_id = db.Column(db.Integer, nullable=False)
    region_filter = db.Column(db.Text)
    kved_filter = db.Column(db.Text)
    size_filter = db.Column(db.Text)
    year_filter = db.Column(db.Integer)
    companies_count = db.Column(db.Integer, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    is_active = db.Column(db.Boolean, default=True)

class RankingCompany(db.Model):
    __tablename__ = 'ranking_companies'
    
    id = db.Column(db.Integer, primary_key=True)
    ranking_id = db.Column(db.Integer, nullable=False)
    company_id = db.Column(db.Integer, nullable=False)
    position = db.Column(db.Integer)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)