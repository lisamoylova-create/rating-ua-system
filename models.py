from app import db
from flask_login import UserMixin
from datetime import datetime

class User(UserMixin, db.Model):
    __tablename__ = 'users'
    
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(256), nullable=False)
    role = db.Column(db.String(20), nullable=False, default='viewer')  # admin, editor, viewer
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    def has_permission(self, permission):
        permissions = {
            'admin': ['view', 'edit', 'upload', 'delete', 'manage_users'],
            'editor': ['view', 'edit', 'upload'],
            'viewer': ['view']
        }
        return permission in permissions.get(self.role, [])

class Region(db.Model):
    __tablename__ = 'regions'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), unique=True, nullable=False)

class Kved(db.Model):
    __tablename__ = 'kved'
    
    id = db.Column(db.Integer, primary_key=True)
    code = db.Column(db.String(10), unique=True, nullable=False)
    description = db.Column(db.String(500), nullable=False)

class CompanySize(db.Model):
    __tablename__ = 'company_sizes'
    
    id = db.Column(db.Integer, primary_key=True)
    size_name = db.Column(db.String(50), unique=True, nullable=False)  # малі, середні, великі

class Company(db.Model):
    __tablename__ = 'companies'
    
    id = db.Column(db.Integer, primary_key=True)
    edrpou = db.Column(db.Text, unique=True, nullable=False, index=True)  # Use TEXT for UTF-8 safety
    name = db.Column(db.Text, nullable=False)
    phone = db.Column(db.Text)
    address = db.Column(db.Text)
    personnel_2019 = db.Column(db.Integer)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    ranking = db.Column(db.Integer)
    ranking_criteria = db.Column(db.Text)  # Критерій за яким проводилося сортування
    
    # Store related data directly to avoid FK issues with UTF-8
    region_name = db.Column(db.Text)
    kved_code = db.Column(db.Text)
    kved_description = db.Column(db.Text)
    company_size_name = db.Column(db.Text)
    
    # Technical columns for export
    source = db.Column(db.Text, default='основний')
    top_count = db.Column(db.Integer)
    total_count = db.Column(db.Integer)
    actualized = db.Column(db.Text, default='ні')
    
    def __repr__(self):
        return f'<Company {self.edrpou}>'

class Financial(db.Model):
    __tablename__ = 'financials'
    
    id = db.Column(db.Integer, primary_key=True)
    year = db.Column(db.Integer, nullable=False)
    revenue = db.Column(db.Numeric(15, 2))  # Чистий дохід від реалізації продукції
    profit = db.Column(db.Numeric(15, 2))   # Чистий фінансовий результат: прибуток
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    # Store company EDRPOU directly instead of FK to avoid UTF-8 issues
    company_edrpou = db.Column(db.Text, nullable=False)
    
    # Unique constraint to prevent duplicate year records for same company
    __table_args__ = (db.UniqueConstraint('company_edrpou', 'year', name='unique_company_year'),)

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
    
    # Relationships
    selection_companies = db.relationship('SelectionCompany', back_populates='selection_base', cascade='all, delete-orphan')
    rankings = db.relationship('Ranking', back_populates='selection_base', cascade='all, delete-orphan')

class SelectionCompany(db.Model):
    __tablename__ = 'selection_companies'
    
    id = db.Column(db.Integer, primary_key=True)
    selection_base_id = db.Column(db.Integer, db.ForeignKey('selection_bases.id'), nullable=False)
    company_id = db.Column(db.Integer, db.ForeignKey('companies.id'), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    # Relationships
    selection_base = db.relationship('SelectionBase', back_populates='selection_companies')
    company = db.relationship('Company')
    
    # Unique constraint
    __table_args__ = (db.UniqueConstraint('selection_base_id', 'company_id', name='unique_selection_company'),)

class Ranking(db.Model):
    __tablename__ = 'rankings'
    
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    selection_base_id = db.Column(db.Integer, db.ForeignKey('selection_bases.id'), nullable=False)
    sort_criteria = db.Column(db.String(50), nullable=False)  # revenue, profit, personnel
    region_filters = db.Column(db.Text)  # JSON array of region IDs
    kved_filters = db.Column(db.Text)   # JSON array of kved IDs
    size_filters = db.Column(db.Text)   # JSON array of size IDs
    companies_count = db.Column(db.Integer, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    is_active = db.Column(db.Boolean, default=True)
    
    # Relationships
    selection_base = db.relationship('SelectionBase', back_populates='rankings')
    ranking_companies = db.relationship('RankingCompany', back_populates='ranking', cascade='all, delete-orphan')

class RankingCompany(db.Model):
    __tablename__ = 'ranking_companies'
    
    id = db.Column(db.Integer, primary_key=True)
    ranking_id = db.Column(db.Integer, db.ForeignKey('rankings.id'), nullable=False)
    company_id = db.Column(db.Integer, db.ForeignKey('companies.id'), nullable=False)
    position = db.Column(db.Integer, nullable=False)
    sort_value = db.Column(db.Numeric(15, 2))  # The value used for sorting
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    # Relationships
    ranking = db.relationship('Ranking', back_populates='ranking_companies')
    company = db.relationship('Company')


class CompanyRankingHistory(db.Model):
    """Історія рейтингів компанії - зберігає всі попередні рейтинги"""
    __tablename__ = 'company_ranking_history'
    
    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.Integer, db.ForeignKey('companies.id'), nullable=False)
    ranking_name = db.Column(db.String(200), nullable=False)
    ranking_position = db.Column(db.Integer, nullable=False)
    ranking_criteria = db.Column(db.String(200), nullable=False)  # Критерій сортування
    source_name = db.Column(db.String(100), nullable=False)  # Источник
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    # Relationship
    company = db.relationship('Company')
    
    # Unique constraint for preventing duplicate history entries
    __table_args__ = (db.UniqueConstraint('company_id', 'ranking_name', 'ranking_criteria', name='unique_company_ranking_history'),)
