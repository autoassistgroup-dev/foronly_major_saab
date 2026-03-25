"""
Configuration Module for AutoAssistGroup Support System

Centralizes all application configuration including:
- Flask settings
- Database connection
- Email settings
- File upload settings
- Session configuration
- Security settings

Author: AutoAssistGroup Development Team
"""

import os
from datetime import timedelta

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


class Config:
    """Base configuration class"""
    
    # Flask Core
    SECRET_KEY = os.environ.get('SECRET_KEY', os.urandom(32).hex())
    DEBUG = os.environ.get('FLASK_ENV') != 'production'
    
    # Environment
    FLASK_ENV = os.environ.get('FLASK_ENV', 'development')
    IS_PRODUCTION = FLASK_ENV == 'production'
    
    # Session Configuration
    SESSION_COOKIE_SECURE = False  # Set to True only in production with HTTPS
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'
    SESSION_REFRESH_EACH_REQUEST = False  # PERFORMANCE: Custom refresh_session() handles this
    PERMANENT_SESSION_LIFETIME = timedelta(days=30)
    SESSION_COOKIE_MAX_AGE = 30 * 24 * 60 * 60  # 30 days in seconds
    
    # File Upload
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB max file size
    ALLOWED_EXTENSIONS = {'pdf', 'doc', 'docx', 'jpg', 'jpeg', 'png', 'txt', 'csv'}
    
    # MongoDB
    MONGODB_URI = os.environ.get('MONGODB_URI')
    
    # Email Configuration
    EMAIL_HOST = os.environ.get('EMAIL_HOST', 'smtp.gmail.com')
    EMAIL_PORT = int(os.environ.get('EMAIL_PORT', '587'))
    EMAIL_USERNAME = os.environ.get('EMAIL_USERNAME', '')
    EMAIL_PASSWORD = os.environ.get('EMAIL_PASSWORD', '')
    EMAIL_USE_TLS = os.environ.get('EMAIL_USE_TLS', 'True').lower() == 'true'
    EMAIL_FROM = os.environ.get('EMAIL_FROM', os.environ.get('EMAIL_USERNAME', ''))
    
    # Webhook
    WEBHOOK_URL = os.environ.get(
        'WEBHOOK_URL', 
        'https://ffxtrading.app.n8n.cloud/webhook/fb4af014-26e6-4477-821f-917fc9b3ee96'
    )
    
    # Upload Folder (must be persistent in production so ticket attachments survive restart)
    @classmethod
    def get_upload_folder(cls):
        """Get upload folder based on environment. Set UPLOAD_FOLDER for persistent storage in production."""
        explicit = os.environ.get('UPLOAD_FOLDER', '').strip()
        if explicit:
            try:
                os.makedirs(explicit, exist_ok=True)
                return explicit
            except OSError:
                pass
        if cls.IS_PRODUCTION:
            upload_folder = '/tmp/uploads'
            try:
                os.makedirs(upload_folder, exist_ok=True)
            except OSError:
                upload_folder = '/tmp'

            return upload_folder
        else:
            # DEVELOPMENT: Use local uploads folder
            path = os.path.join(os.getcwd(), 'uploads')
            upload_folder = os.environ.get('UPLOAD_FOLDER', path)
            
            try:
                os.makedirs(upload_folder, exist_ok=True)
                print(f"📂 [CONFIG] Using upload folder: {upload_folder}")
            except OSError as e:
                print(f"⚠️ [CONFIG] Failed to create {upload_folder}: {e}")
                import tempfile
                upload_folder = os.path.join(tempfile.gettempdir(), 'autoassist_uploads')
                os.makedirs(upload_folder, exist_ok=True)
                print(f"📂 [CONFIG] Fallback to temp upload folder: {upload_folder}")
            return upload_folder


class DevelopmentConfig(Config):
    """Development configuration"""
    DEBUG = True
    SESSION_COOKIE_SECURE = False


class ProductionConfig(Config):
    """Production configuration"""
    DEBUG = False
    SESSION_COOKIE_SECURE = True
    
    def __init__(self):
        if not self.SECRET_KEY or self.SECRET_KEY == os.urandom(32).hex():
            raise ValueError("SECRET_KEY must be set in production environment")


class TestingConfig(Config):
    """Testing configuration"""
    TESTING = True
    DEBUG = True


# Configuration factory
config_map = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}


def get_config():
    """Get configuration based on environment"""
    env = os.environ.get('FLASK_ENV', 'development')
    return config_map.get(env, config_map['default'])()


# For backwards compatibility - expose commonly used values directly
UPLOAD_FOLDER = Config.get_upload_folder()
ALLOWED_EXTENSIONS = Config.ALLOWED_EXTENSIONS
WEBHOOK_URL = Config.WEBHOOK_URL
EMAIL_HOST = Config.EMAIL_HOST
EMAIL_PORT = Config.EMAIL_PORT
EMAIL_USERNAME = Config.EMAIL_USERNAME
EMAIL_PASSWORD = Config.EMAIL_PASSWORD
EMAIL_USE_TLS = Config.EMAIL_USE_TLS
EMAIL_FROM = Config.EMAIL_FROM
