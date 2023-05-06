import os
from flask import Flask
from flask_cors import CORS
from DocuHive.database.setup import db
import DocuHive.database.models
from DocuHive.backend_utils.seed import seed_tags_labels_and_workflows
from DocuHive.rest_routes.routes import bp
from DocuHive.graphql_routes.schema import schema
from strawberry.flask.views import GraphQLView
from dotenv import load_dotenv
load_dotenv()


def create_app():
    app = Flask(__name__)
    DB_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://psql:0000@localhost:5432/docuhive")
    app.config["SQLALCHEMY_DATABASE_URI"] = DB_URL
    app.config.update(
        CELERY_CONFIG={
            "broker": os.getenv("CELERY_BROKER_URL", "amqp://localhost"),
            "backend": os.getenv("CELERY_RESULT_BACKEND", "rpc://"),
            "task_track_started": True,
            "broker_heartbeat": 300,
            "broker_heartbeat_checkrate": 1,
        }
    )

    CORS(app)
    app.register_blueprint(bp)
    app.add_url_rule(
        "/graphql",
        view_func=GraphQLView.as_view("graphql_view", schema=schema),
    )
    db.init_app(app)
    with app.app_context():
        db.create_all()
        seed_tags_labels_and_workflows()
    return app
