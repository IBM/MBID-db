from datetime import datetime
import sys
import enum
sys.path.append("..")
from app import db

class TeslasEnum(enum.Enum):
    one_and_a_half = 1
    three = 2

class Scanner(db.Model):
    __tablename__ = 'scanner'
    id = db.Column(db.Integer, primary_key=True)
    brand = db.Column(db.String(255))
    model = db.Column(db.String(255))
    source_id = db.Column(db.String(255))
    teslas = db.Column(db.Enum(TeslasEnum))
    source_dataset_id = db.Column(db.Integer, db.ForeignKey('source_dataset.id'), index=True)
    image = db.relationship('Image', backref='scanner', lazy='dynamic', cascade='all, delete-orphan')
    deleted_at = db.Column(db.DateTime, default=None)
    created_at = db.Column(db.DateTime, default=datetime.utcnow())
    updated_at = db.Column(db.DateTime, default=datetime.utcnow(),
                           onupdate=datetime.utcnow())
