from datetime import datetime
import sys
import enum
sys.path.append("..")
from app import db


class FileType(db.Model):
    __tablename__ = 'file_type'
    id = db.Column(db.Integer, primary_key=True)
    designation = db.Column(db.String(255))
    preprocess_task_file = db.relationship('PreprocessTaskFile', backref='file_type', lazy='dynamic', cascade='all, delete-orphan')
    deleted_at = db.Column(db.DateTime, default=None)
    created_at = db.Column(db.DateTime, default=datetime.utcnow())
    updated_at = db.Column(db.DateTime, default=datetime.utcnow(),
                           onupdate=datetime.utcnow())
