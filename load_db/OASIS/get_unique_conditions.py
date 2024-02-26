import pandas as pd
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from app import create_app, db
from app.models import Subject, SourceDataset
from config.globals import ENVIRONMENT
import pdb



if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        clinical_data_df = pd.read_csv('../../data/OASIS_metadata/clinical_data.csv')
        pd.Series(clinical_data_df['dx1'].unique()).to_csv('unique_dx1.csv',index=False)
