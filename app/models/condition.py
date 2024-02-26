from datetime import datetime
import sys
sys.path.append("..")
from app import db


class Condition(db.Model):
    __tablename__ = 'condition'
    id = db.Column(db.Integer, primary_key=True)
    designation = db.Column(db.String(255))
    subject = db.relationship('Subject', backref='condition', lazy='dynamic', cascade='all, delete-orphan')
    source_dataset_condition = db.relationship('SourceDatasetCondition', backref='condition',
                                               lazy='dynamic', cascade='all, delete-orphan')
    visit = db.relationship('Visit', backref='condition',
                                               lazy='dynamic', cascade='all, delete-orphan')
    deleted_at = db.Column(db.DateTime, default=None)
    created_at = db.Column(db.DateTime, default=datetime.utcnow())
    updated_at = db.Column(db.DateTime, default=datetime.utcnow(),
                           onupdate=datetime.utcnow())
