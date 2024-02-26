import sys
import os
import numpy as np
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from app.models import Condition


def get_ppmi_condition(diagnosis):
    all_diagnoses = ["Prodromal PD", "Healthy Control", "Parkinson's Disease"]
    condition = None
    if np.any([diagnosis == diagn for diagn in all_diagnoses]):
        condition = Condition.query.filter(Condition.designation ==
                                           diagnosis).first()
    return condition
