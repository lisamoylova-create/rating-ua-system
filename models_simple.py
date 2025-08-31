from app import db
from flask_login import UserMixin
from datetime import datetime

class User(UserMixin, db.Model):
    __tablename__ = 'users'
    
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(256), nullable=False)
    role = db.Column(db.String(20), nullable=False, default='viewer')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    def has_permission(self, permission):
        permissions = {
            'admin': ['view', 'edit', 'upload', 'delete', 'manage_users'],
            'editor': ['view', 'edit', 'upload'],
            'viewer': ['view']
        }
        return permission in permissions.get(self.role, [])

class Company(db.Model):
    __tablename__ = 'companies'
    
    id = db.Column(db.Integer, primary_key=True)
    edrpou = db.Column(db.String(20), unique=True, nullable=False, index=True)
    name = db.Column(db.String(500), nullable=False)
    phone = db.Column(db.String(50))
    address = db.Column(db.String(1000))
    personnel_2019 = db.Column(db.Integer)
    
    # Store all data directly as strings to avoid FK issues
    region_name = db.Column(db.String(200))
    kved_code = db.Column(db.String(20))
    kved_description = db.Column(db.String(500))
    company_size_name = db.Column(db.String(50))
    
    # Financial data stored directly
    revenue_2019 = db.Column(db.Numeric(15, 2))
    profit_2019 = db.Column(db.Numeric(15, 2))
    
    # Technical fields
    source = db.Column(db.String(50), default='основний')
    top_count = db.Column(db.Integer)
    total_count = db.Column(db.Integer)
    actualized = db.Column(db.String(10), default='ні')
    ranking = db.Column(db.Integer)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return f'<Company {self.edrpou}>'

class SelectionBase(db.Model):
    __tablename__ = 'selection_bases'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False, default='База для ранжування')
    min_employees = db.Column(db.Integer, default=0)
    min_revenue = db.Column(db.Numeric(15, 2), default=0)
    min_profit = db.Column(db.Numeric(15, 2))
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
    name = db.Column(db.String(255), nullable=False)
    selection_base_id = db.Column(db.Integer, nullable=False)
    region_filter = db.Column(db.String(200))
    kved_filter = db.Column(db.String(20))
    size_filter = db.Column(db.String(50))
    year_filter = db.Column(db.Integer, default=2019)
    companies_count = db.Column(db.Integer, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    is_active = db.Column(db.Boolean, default=True)

class RankingCompany(db.Model):
    __tablename__ = 'ranking_companies'
    
    id = db.Column(db.Integer, primary_key=True)
    ranking_id = db.Column(db.Integer, nullable=False)
    company_id = db.Column(db.Integer, nullable=False)
    position = db.Column(db.Integer, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)