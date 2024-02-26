import sys
import os
import numpy as np
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from app.models import Condition


def get_openpain_condition(diagnosis):
    all_diagnoses = ['healthy', 'subacute', 'chronic']
    condition_vals = ['Healthy Control', 'Subacute Pain', 'Chronic Pain']
    cond_map = dict(zip(all_diagnoses, condition_vals))
    condition = None
    if np.any([diagnosis == diagn for diagn in all_diagnoses]):
        condition = Condition.query.filter(Condition.designation ==
                                           cond_map[diagnosis]).first()
    return condition




