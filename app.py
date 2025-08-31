import os
import logging
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager
from sqlalchemy.orm import DeclarativeBase
from werkzeug.middleware.proxy_fix import ProxyFix

# Set up logging
logging.basicConfig(level=logging.DEBUG)

class Base(DeclarativeBase):
    pass

db = SQLAlchemy(model_class=Base)
login_manager = LoginManager()

# Create the app
app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "dev-secret-key-change-in-production")
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

# Configure the database with production-safe defaults
database_url = os.environ.get("DATABASE_URL")
if not database_url:
    # Fallback for production if DATABASE_URL is not set
    # Replit automatically provides database connection
    database_url = "postgresql://username:password@hostname:port/database"
    logging.warning("DATABASE_URL not found, using default configuration")

app.config["SQLALCHEMY_DATABASE_URI"] = database_url
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
    "pool_recycle": 300,
    "pool_pre_ping": True,
    "pool_timeout": 20,
    "pool_size": 10,
    "max_overflow": 20
}
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Initialize extensions
db.init_app(app)
login_manager.init_app(app)
login_manager.login_view = 'auth.login'
login_manager.login_message = 'Будь ласка, увійдіть для доступу до цієї сторінки.'

# Create upload directory
os.makedirs('uploads', exist_ok=True)

def initialize_app():
    """Initialize app with error handling for deployment"""
    try:
        with app.app_context():
            # Import full models with all 33 columns
            import models_full
            
            # Create tables with error handling
            try:
                db.create_all()
                logging.info("Database tables created successfully")
            except Exception as e:
                logging.error(f"Error creating database tables: {e}")
                # Don't fail completely, tables might already exist
            
            # Create default admin user if it doesn't exist
            try:
                from werkzeug.security import generate_password_hash
                from models_full import User
                
                admin = db.session.execute(db.select(User).where(User.username == 'admin')).scalar_one_or_none()
                if not admin:
                    admin_user = User(
                        username='admin',
                        email='admin@example.com',
                        password_hash=generate_password_hash('admin123'),
                        role='admin'
                    )
                    db.session.add(admin_user)
                    db.session.commit()
                    logging.info("Default admin user created: admin/admin123")
            except Exception as e:
                logging.error(f"Error creating admin user: {e}")
                # Non-critical, user can be created later
            
# Автоматичне виправлення даних актуалізації ВИМКНЕНО
            # Цей код скидав дані актуалізації при кожному старті
            # try:
            #     incorrect_count = db.session.execute(db.text(
            #         "SELECT COUNT(*) FROM companies WHERE actualized = 'так'"
            #     )).scalar()
            #     
            #     if incorrect_count > 0:
            #         logging.info(f"Виправляю {incorrect_count} некоректних записів актуалізації...")
            #         db.session.execute(db.text(
            #             "UPDATE companies SET source = 'основний', actualized = 'ні'"
            #         ))
            #         db.session.commit()
            #         logging.info("✅ Дані актуалізації виправлено автоматично")
            # except Exception as e:
            #     logging.error(f"Помилка при автоматичному виправленні даних: {e}")
            #     db.session.rollback()
    except Exception as e:
        logging.error(f"Error initializing app: {e}")
        # Don't crash the app, let it start even with database issues

# Initialize the app
initialize_app()

# Register API blueprints
try:
    from api_endpoints import api_filters
    app.register_blueprint(api_filters)
    logging.info("API endpoints registered successfully")
except Exception as e:
    logging.error(f"Error registering API endpoints: {e}")

# Register blueprints at the end to avoid circular import
# This will be called after all modules are fully initialized

@login_manager.user_loader
def load_user(user_id):
    from models_full import User
    return db.session.execute(db.select(User).where(User.id == int(user_id))).scalar_one_or_none()
