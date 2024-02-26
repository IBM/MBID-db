from datetime import datetime
import sys
sys.path.append("..")
from app import db
from sqlalchemy.dialects.postgresql import JSON



class Visit(db.Model):
    __tablename__ = 'visit'
    id = db.Column(db.Integer, primary_key=True)
    external_id = db.Column(db.String(255))
    subject_id = db.Column(db.Integer, db.ForeignKey('subject.id'), index=True)
    source_dataset_id = db.Column(db.Integer, db.ForeignKey('source_dataset.id'), index=True)
    condition_id = db.Column(db.Integer, db.ForeignKey('condition.id'), index=True)
    symptoms = db.Column(JSON)
    description = db.Column(db.String(255))
    days_since_baseline = db.Column(db.Integer)
    bmi = db.Column(db.Float)
    image = db.relationship('Image', backref='visit', lazy='dynamic', cascade='all, delete-orphan')
    deleted_at = db.Column(db.DateTime, default=None)
    created_at = db.Column(db.DateTime, default=datetime.utcnow())
    updated_at = db.Column(db.DateTime, default=datetime.utcnow(),
                           onupdate=datetime.utcnow())
