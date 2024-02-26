from datetime import datetime
import sys
sys.path.append("..")
from app import db


class AuthKeys(db.Model):
    __tablename__ = 'auth_keys'
    id = db.Column(db.Integer, primary_key=True)
    designation = db.Column(db.String(150))
    guid = db.Column(db.String(255))
    deleted_at = db.Column(db.DateTime, default=None)
    created_at = db.Column(db.DateTime, default=datetime.utcnow())
    updated_at = db.Column(db.DateTime, default=datetime.utcnow())
