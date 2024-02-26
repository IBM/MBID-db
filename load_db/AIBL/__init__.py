import sys
import os
import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from app.models import Condition

def get_aibl_dx(visit_dx, symptoms):
    condition = None
    # At this moment, all subjects in the AIBL dataset are healthy or present AD or MCI only
    # even though the metadata contains values for other conditions
    if visit_dx == 1:
        # In case CDR is 0.5 or higher and the diagnostic is Healthy Control, change it to MCI
        if 'cdr-global' in symptoms and symptoms['cdr-global'] >= 0.5:
            condition = Condition.query.filter(Condition.designation == "Mild Cognitive Impairment").first()
        else:
            condition = Condition.query.filter(Condition.designation == "Healthy Control").first()
    elif visit_dx == 2:
        condition = Condition.query.filter(Condition.designation == "Mild Cognitive Impairment").first()
    elif visit_dx == 3:
        condition = Condition.query.filter(Condition.designation == "Alzheimer's Disease").first()
    return condition

def extract_symptoms(row):
    symp_dict = dict()
    if pd.notnull(row['MMSCORE']):
        symp_dict['mmse'] = row['MMSCORE']
    else:
        symp_dict['mmse'] = None
    if pd.notnull(row['CDGLOBAL']):
        symp_dict['cdr-global'] = row['CDGLOBAL']
    else:
        symp_dict['cdr-global'] = None
    return symp_dict
