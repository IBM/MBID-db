import pandas as pd
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from app import create_app, db
from app.models import Condition
from config.globals import ENVIRONMENT
from pathlib import Path



if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        condition_df = pd.read_csv((Path(__file__).parent / '../../data/seed_files/condition.csv'))
        for index, row in condition_df.iterrows():
            existing_condition = Condition.query.filter(Condition.designation == row.designation).first()
            if existing_condition:
                continue
            else:
                condition_db = Condition(designation=row.designation)
                db.session.add(condition_db)
            db.session.commit()
