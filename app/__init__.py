"""
Portfolio Data Clearinghouse - Flask Application Factory
"""
from flask import Flask
from .extensions import db
from .config import Config


def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)

    db.init_app(app)

    from .routes.ingest import ingest_bp
    from .routes.positions import positions_bp
    from .routes.compliance import compliance_bp
    from .routes.reconciliation import reconciliation_bp

    app.register_blueprint(ingest_bp)
    app.register_blueprint(positions_bp)
    app.register_blueprint(compliance_bp)
    app.register_blueprint(reconciliation_bp)

    with app.app_context():
        db.create_all()

    return app
