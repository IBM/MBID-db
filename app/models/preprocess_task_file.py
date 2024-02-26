from datetime import datetime
import sys
import enum
sys.path.append("..")
from app import db
from sqlalchemy.dialects.postgresql import JSON


class PreprocessTaskFile(db.Model):
    __tablename__ = 'preprocess_task_file'
    id = db.Column(db.Integer, primary_key=True)
    image_id = db.Column(db.Integer, db.ForeignKey('image.id'), index=True)
    preprocess_task_id = db.Column(db.Integer, db.ForeignKey('preprocess_task.id'), index=True)
    file_type_id = db.Column(db.Integer, db.ForeignKey('file_type.id'), index=True)
    preprocess_file_path = db.Column(db.String(255))
    preprocess_check = db.Column(db.Boolean)
    preprocess_check_json = db.Column(JSON)
    deleted_at = db.Column(db.DateTime, default=None)
    created_at = db.Column(db.DateTime, default=datetime.utcnow())
    updated_at = db.Column(db.DateTime, default=datetime.utcnow(),
                           onupdate=datetime.utcnow())
