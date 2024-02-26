import pandas as pd
import json
import os
import pdb

template_json_file = None
for subdir, dirs, files in os.walk('../../temp/unzip_files/unzipped'):
    for file in files:
        if file.endswith(".json"):
            with open(subdir+'/'+file) as f:
                image_metadata_json = json.load(f)
            image_metadata_json.pop('AcquisitionTime', None)
            image_metadata_json.pop('AcquisitionDate', None)
            if not template_json_file:
                template_json_file = image_metadata_json
            else:
                print(template_json_file == image_metadata_json)
