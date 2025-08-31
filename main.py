from app import app

# Register blueprints after app is fully initialized
from routes import main
from auth import auth
from api import api

app.register_blueprint(main)
app.register_blueprint(auth, url_prefix='/auth')
app.register_blueprint(api, url_prefix='/api')

# Startup fix видалений - автоматичне виправлення даних тепер в app.py

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)