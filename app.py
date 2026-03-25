# Eventlet for async SocketIO (Linux/macOS). Skip on Vercel serverless & Windows.
import sys
import os
_is_serverless = os.environ.get('VERCEL') or os.environ.get('AWS_LAMBDA_FUNCTION_NAME')
if sys.platform != 'win32' and not _is_serverless:
    import eventlet
    eventlet.monkey_patch()

"""
AutoAssistGroup Support Ticket Management System - Refactored Entry Point

This is the new modular version of the application using Flask blueprints
and a service-oriented architecture.

Author: AutoAssistGroup Development Team
Version: 3.0 (Refactored)
"""

import os
import logging
from flask import Flask
from flask_cors import CORS
from datetime import timedelta

# Import configuration
from config.settings import Config, get_config

# Import middleware
from middleware.error_handlers import register_error_handlers
from middleware.session_manager import refresh_session, check_and_restore_session

# Import routes
from routes import register_blueprints

# Import template filters
from utils.template_filters import register_template_filters

# Import SocketIO for real-time updates
from socket_events import socketio, init_socketio


def create_app(config_class=None):
    """
    Application factory for creating Flask app instances.
    
    Args:
        config_class: Configuration class to use (default from environment)
        
    Returns:
        Flask application instance
    """
    # Create Flask app
    app = Flask(__name__)
    
    # Load configuration
    if config_class is None:
        config = get_config()
    else:
        config = config_class()
    
    # Apply configuration
    app.config.from_object(config)
    app.secret_key = config.SECRET_KEY
    
    # Session configuration
    app.config['SESSION_COOKIE_SECURE'] = config.SESSION_COOKIE_SECURE
    app.config['SESSION_COOKIE_HTTPONLY'] = config.SESSION_COOKIE_HTTPONLY
    app.config['SESSION_COOKIE_SAMESITE'] = config.SESSION_COOKIE_SAMESITE
    app.config['SESSION_REFRESH_EACH_REQUEST'] = config.SESSION_REFRESH_EACH_REQUEST
    app.config['PERMANENT_SESSION_LIFETIME'] = config.PERMANENT_SESSION_LIFETIME
    app.config['MAX_CONTENT_LENGTH'] = config.MAX_CONTENT_LENGTH
    
    # PERFORMANCE: Cache static files in browser for 12 hours (CSS, JS, images)
    app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 43200  # 12 hours in seconds
    
    # Enable CORS
    CORS(app)
    
    # Configure logging
    _configure_logging(app)
    
    # Register error handlers
    register_error_handlers(app)
    
    # Register template filters
    register_template_filters(app)
    
    # Register blueprints (all routes are now in blueprints)
    register_blueprints(app)
    
    # Register request hooks
    _register_request_hooks(app)
    
    # Add security headers
    @app.after_request
    def set_security_headers(response):
        response.headers['X-Content-Type-Options'] = 'nosniff'
        return response
    
    # Add favicon route
    @app.route('/favicon.ico')
    def favicon():
        """Handle favicon requests."""
        return app.send_static_file('favicon.ico') if os.path.exists(
            os.path.join(app.static_folder, 'favicon.ico')
        ) else ('', 204)
    
    # N8N compatibility: /webhook/reply forwards to /api/webhook/reply
    # The "Mail => Portal" workflow sends customer replies to this endpoint
    @app.route('/webhook/reply', methods=['POST'])
    def webhook_reply_alias():
        """Forward webhook/reply to api/webhook/reply for N8N compatibility."""
        from routes.webhook_routes import webhook_reply
        return webhook_reply()
    
    # Initialize SocketIO for real-time updates
    init_socketio(app)
    
    app.logger.info("Application initialized successfully (refactored version)")
    
    return app


def _configure_logging(app):
    """Configure application logging."""
    is_production = os.environ.get('FLASK_ENV') == 'production'
    log_level = logging.INFO if is_production else logging.DEBUG
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    
    # Reduce noise from third-party libraries
    logging.getLogger('werkzeug').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('pymongo').setLevel(logging.WARNING)
    
    # Add file handler in development
    if not is_production:
        try:
            file_handler = logging.FileHandler('app.log')
            file_handler.setFormatter(
                logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            )
            logging.getLogger().addHandler(file_handler)
        except Exception:
            pass


def _register_request_hooks(app):
    """Register before/after request hooks."""
    
    @app.before_request
    def before_request():
        """Handle session management before each request - OPTIMIZED."""
        from flask import request, session
        
        # Skip for static files
        if request.endpoint in ['static', 'favicon'] or request.path.startswith('/static/'):
            return None
        
        # Skip for health checks
        if request.endpoint in ['health.health_check', 'health.api_status']:
            return None
            
        # PERFORMANCE: system_settings uses DB-level caching now (5-min TTL)
        # so this is only a real DB call once every 5 minutes, not every request
        from database import get_db
        try:
            db = get_db()
            app.jinja_env.globals['system_settings'] = db.get_system_settings()
        except Exception:
            app.jinja_env.globals['system_settings'] = {'show_background': True}
        
        # Try to restore session if needed (no DB call if session exists)
        if 'member_id' not in session:
            check_and_restore_session()
        
        # PERFORMANCE: Lightweight refresh - only writes to session every 5 minutes
        if 'member_id' in session:
            refresh_session()


# Create the application
app = create_app()


if __name__ == '__main__':
    # Get port from environment or default to 5000
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_ENV') != 'production'
    
    banner = (
        f"\n  AutoAssistGroup Support System - Refactored Version\n"
        f"  Running on: http://localhost:{port}\n"
        f"  Debug mode: {str(debug).lower()}\n"
        f"  Environment: {os.environ.get('FLASK_ENV', 'development')}\n"
    )
    print(banner)
    
    # Use SocketIO to run the app for WebSocket support
    # Note: use_reloader=False is required when using eventlet async mode
    socketio.run(app, host='0.0.0.0', port=port, debug=debug, use_reloader=False)
