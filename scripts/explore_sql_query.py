#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 30 22:05:33 2022

@author: ecastrow
"""

import sqlalchemy as db
import pandas as pd


# Default setup to load a database
engine = db.create_engine('sqlite:////data2/eduardo/code'
                          '/xls_preprocessing/app/xls.db')

connection = engine.connect()
metadata = db.MetaData()

# Load subject table
subj_table = db.Table('subject', metadata, autoload=True, autoload_with=engine)
image_table = db.Table('image', metadata, autoload=True, autoload_with=engine)
visit_table = db.Table('visit', metadata, autoload=True, autoload_with=engine)

# Check table's columns types
col_types = [{col.name: col.type} for col in subj_table.columns]

# Do test query on subject table
query = db.select([subj_table.columns.id, subj_table.columns.external_id,
                   subj_table.columns.age_at_baseline, subj_table.columns.gender,
                   subj_table.columns.education_yrs, subj_table.columns.race,
                   subj_table.columns.condition_id,
                   subj_table.columns.source_dataset_id]).\
    where(subj_table.columns.external_id == '14426')
    #filter(subj_table.columns.external_id.contains('300'))
result = connection.execute(query).fetchall()

# Alternative query method
subj_df = pd.read_sql('SELECT external_id, age_at_baseline, gender, '
                      'education_yrs, race, condition_id, '
                      'source_dataset_id FROM subject', connection)
subj_df['external_id'] = subj_df['external_id'].astype(int)
subj_df = subj_df.sort_values(by='external_id')

# Do test query on image table
query = db.select([image_table.columns.id, image_table.columns.subject_id,
                   image_table.columns.image_path,
                   image_table.columns.visit_id,
                   image_table.columns.scanner_id,
                   image_table.columns.type,
                   #image_table.columns.metadata_json,
                   image_table.columns.days_since_baseline,
                   image_table.columns.source_dataset_id]).\
    filter(image_table.columns.image_path.contains('S405052'))
result = connection.execute(query).fetchall()

# Alternative query method
img_df = pd.read_sql('SELECT subject_id, image_path, visit_id, scanner_id, '
                     'type, days_since_baseline, source_dataset_id,'
                     'metadata_json FROM '
                     'image WHERE subject_id == 618', connection)

# Do test query on visit table
query = db.select([visit_table.columns.id,
                   visit_table.columns.external_id,
                   visit_table.columns.subject_id,
                   visit_table.columns.source_dataset_id,
                   visit_table.columns.condition_id,
                   visit_table.columns.symptoms,
                   visit_table.columns.days_since_baseline,
                   visit_table.columns.bmi]).\
    where(visit_table.columns.subject_id == 618)
result = connection.execute(query).fetchall()

# Alternative query method
visit_df = pd.read_sql('SELECT external_id, subject_id, source_dataset_id, '
                       'condition_id, symptoms, days_since_baseline, bmi FROM '
                       'visit', connection)







