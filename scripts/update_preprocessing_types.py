import sqlalchemy as db
import pandas as pd
import os
from app import create_app
from config.globals import ENVIRONMENT


# Set up connection to DB
current_app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
engine = db.create_engine(current_app.config['SQLALCHEMY_DATABASE_URI'])

# Execute alter command to update enum class
alter_enum = "ALTER TYPE preprocessedenum ADD VALUE 'warping'"
with engine.connect() as conn:
    conn.execute(alter_enum)

# Execute alter command to update image column (preprocessed)
alter_coltype = 'ALTER TABLE image ALTER COLUMN preprocessed TYPE preprocessedenum'
with engine.connect() as conn:
    conn.execute(alter_coltype)

