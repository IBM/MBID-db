import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from app.models import Condition

def get_adni_dx(visit_dx):
    condition = None
    if not isinstance(visit_dx, str):
        return None
    if visit_dx == 'CN' or visit_dx == 'SMC':
        condition = Condition.query.filter(Condition.designation == "Healthy Control").first()
    elif visit_dx == 'AD' or visit_dx == 'Dementia':
        condition = Condition.query.filter(Condition.designation == "Alzheimer's Disease").first()
    elif 'MCI' in visit_dx:
        condition = Condition.query.filter(Condition.designation == "Mild Cognitive Impairment").first()
    return condition

def get_visit_code(row):
    visit = row['Visit'].lower()
    if visit == 'no visit defined':
        return None
    if 'baseline' in visit or 'initial visit' in visit or 'screening' in visit:
        return 'bl'
    elif ('month 3' in visit and not 'month 36' in visit) or ('month 6' in visit and not 'month 60' in visit):
        return 'm06'
    elif 'month 12' in visit or 'year 1' in visit:
        return 'm12'
    elif 'month 18' in visit:
        return 'm18'
    elif 'month 24' in visit or 'year 2' in visit:
        return 'm24'
    elif 'month 30' in visit:
        return 'm30'
    elif 'month 36' in visit or 'year 3' in visit:
        return 'm36'
    elif 'month 42' in visit:
        return 'm42'
    elif 'month 48' in visit or 'year 4' in visit:
        return 'm48'
    elif 'month 54' in visit:
        return 'm54'
    elif 'month 60' in visit or 'year 5' in visit:
        return 'm60'
    elif 'month 66' in visit:
        return 'm66'
    elif 'month 72' in visit or 'year 6' in visit:
        return 'm72'
    elif 'month 78' in visit:
        return 'm78'
