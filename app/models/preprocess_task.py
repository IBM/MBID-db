from datetime import datetime
import sys
import enum
sys.path.append("..")
from app import db
from sqlalchemy.dialects.postgresql import JSON


class PreprocessTask(db.Model):
    __tablename__ = 'preprocess_task'
    id = db.Column(db.Integer, primary_key=True)
    description = db.Column(db.String(255))
    preprocess_task_image = db.relationship('PreprocessTaskFile', backref='preprocess_task', lazy='dynamic', cascade='all, delete-orphan')
    deleted_at = db.Column(db.DateTime, default=None)
    created_at = db.Column(db.DateTime, default=datetime.utcnow())
    updated_at = db.Column(db.DateTime, default=datetime.utcnow(),
                           onupdate=datetime.utcnow())
