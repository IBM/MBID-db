from datetime import datetime
import sys
sys.path.append("..")
from app import db


class SourceDatasetCondition(db.Model):
    __tablename__ = 'source_dataset_condition'
    id = db.Column(db.Integer, primary_key=True)
    source_dataset_id = db.Column(db.Integer, db.ForeignKey('source_dataset.id'), index=True)
    condition_id = db.Column(db.Integer, db.ForeignKey('condition.id'), index=True)

    deleted_at = db.Column(db.DateTime, default=None)
    created_at = db.Column(db.DateTime, default=datetime.utcnow())
    updated_at = db.Column(db.DateTime, default=datetime.utcnow(),
                           onupdate=datetime.utcnow())
