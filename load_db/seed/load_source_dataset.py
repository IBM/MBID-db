import pandas as pd
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from app import create_app, db
from app.models import SourceDataset
from config.globals import ENVIRONMENT
from pathlib import Path



if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        source_dataset_df = pd.read_csv((Path(__file__).parent / '../../data/seed_files/source_dataset.csv'))
        for index, row in source_dataset_df.iterrows():
            existing_source_dataset = SourceDataset.query.filter(SourceDataset.designation == row.designation).first()
            if existing_source_dataset:
                existing_source_dataset.description = row.description
                db.session.merge(existing_source_dataset)
            else:
                source_dataset_db = SourceDataset(designation=row.designation, description=row.description)
                db.session.add(source_dataset_db)
            db.session.commit()
