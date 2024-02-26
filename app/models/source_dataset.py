from datetime import datetime
import sys
sys.path.append("..")
from app import db


class SourceDataset(db.Model):
    __tablename__ = 'source_dataset'
    id = db.Column(db.Integer, primary_key=True)
    designation = db.Column(db.String(255), unique=True)
    description = db.Column(db.String(255))
    subject = db.relationship('Subject', backref='source_dataset', lazy='dynamic', cascade='all, delete-orphan')
    visit = db.relationship('Visit', backref='source_dataset', lazy='dynamic', cascade='all, delete-orphan')
    image = db.relationship('Image', backref='source_dataset', lazy='dynamic', cascade='all, delete-orphan')
    source_dataset_condition = db.relationship('SourceDatasetCondition', backref='source_dataset',
                                               lazy='dynamic', cascade='all, delete-orphan')
    scanner = db.relationship('Scanner', backref='source_dataset', lazy='dynamic', cascade='all, delete-orphan')
    deleted_at = db.Column(db.DateTime, default=None)
    created_at = db.Column(db.DateTime, default=datetime.utcnow())
    updated_at = db.Column(db.DateTime, default=datetime.utcnow(),
                           onupdate=datetime.utcnow())
