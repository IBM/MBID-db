from datetime import datetime
import sys
import enum
sys.path.append("..")
from app import db
from sqlalchemy.dialects.postgresql import JSON


class TypeEnum(enum.Enum):
    T1 = 1
    DWI = 2
    rsfMRI = 3
    FieldMap = 4

class PreprocessedEnum(enum.Enum):
    none = 0
    minimal1 = 1
    locked = 2
    failed = 3
    warping = 4


class Image(db.Model):
    __tablename__ = 'image'
    id = db.Column(db.Integer, primary_key=True)
    subject_id = db.Column(db.Integer, db.ForeignKey('subject.id'), index=True)
    image_path = db.Column(db.String(255))
    file_size = db.Column(db.Float)
    visit_id = db.Column(db.Integer, db.ForeignKey('visit.id'), index=True)
    scanner_id = db.Column(db.Integer, db.ForeignKey('scanner.id'), index=True)
    preprocessed = db.Column(db.Enum(PreprocessedEnum))
    # Id of the process that block the image in order to preprocess it
    blocking_processing_id = db.Column(db.Integer)
    type = db.Column(db.Enum(TypeEnum))
    metadata_json = db.Column(JSON)
    days_since_baseline = db.Column(db.Integer)
    source_dataset_id = db.Column(db.Integer, db.ForeignKey('source_dataset.id'), index=True)
    preprocess_task_file = db.relationship('PreprocessTaskFile', backref='image', lazy='dynamic', cascade='all, delete-orphan')
    deleted_at = db.Column(db.DateTime, default=None)
    created_at = db.Column(db.DateTime, default=datetime.utcnow())
    updated_at = db.Column(db.DateTime, default=datetime.utcnow(),
                           onupdate=datetime.utcnow())
