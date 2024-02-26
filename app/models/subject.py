from datetime import datetime
import sys
import enum
sys.path.append("..")
from app import db

class GenderEnum(enum.Enum):
    male = 1
    female = 2
    undefined = 3

class HandEnum(enum.Enum):
    left = 1
    right = 2
    ambidextrous = 3
    undefined = 4

class RaceEnum(enum.Enum):
    white_non_latino = 1
    white_latino = 2
    black = 3
    asian_or_pacific = 4
    american_indian = 5
    multiple = 6
    undefined = 7

class Subject(db.Model):
    __tablename__ = 'subject'
    id = db.Column(db.Integer, primary_key=True)
    external_id = db.Column(db.String(255))
    age_at_baseline = db.Column(db.Float)
    gender = db.Column(db.Enum(GenderEnum))
    hand = db.Column(db.Enum(HandEnum))
    education_yrs = db.Column(db.Integer)
    race = db.Column(db.Enum(RaceEnum))
    condition_id = db.Column(db.Integer, db.ForeignKey('condition.id'), index=True)
    source_dataset_id = db.Column(db.Integer, db.ForeignKey('source_dataset.id'), index=True)
    visit = db.relationship('Visit', backref='subject', lazy='dynamic', cascade='all, delete-orphan')
    deleted_at = db.Column(db.DateTime, default=None)
    created_at = db.Column(db.DateTime, default=datetime.utcnow())
    updated_at = db.Column(db.DateTime, default=datetime.utcnow(),
                           onupdate=datetime.utcnow())
