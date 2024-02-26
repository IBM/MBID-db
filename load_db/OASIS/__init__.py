import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from app.models import Condition

def get_oasis_dx(visit_cdr, visit_dx1):
    condition = None
    if visit_cdr == 0:
        condition = Condition.query.filter(Condition.designation == "Healthy Control").first()
    elif visit_cdr == 0.5:
        condition = Condition.query.filter(
            Condition.designation == "Mild Cognitive Impairment").first()
    elif visit_cdr >= 1:
        if ('ad' in visit_dx1 and not 'non ad' in visit_dx1 and not 'no ad' in visit_dx1) or visit_dx1 == '.':
            condition = Condition.query.filter(Condition.designation == "Alzheimer's Disease").first()
        else:
            condition = Condition.query.filter(Condition.designation == "Dementia Unspecified").first()
    return condition