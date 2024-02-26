### MBID-db: Database for fusion of biomedical imaging studies

### Basic commands and miscellaneous information
* Script `run.py` will start Flask API and create DB. In order to create all tables, is necessary to run this command. `python run.py`
* Script `flask db migrate` will check changes in the db, and command `flask db upgrade` will apply the required changes
* All new folders that contain python code, need to have a file `__init__.py` in the directory (most of them will be empty files)
* All table table definitions needs to be imported in the `__init__.py` that lives in `app/models/__init__.py`
### DB structure and definition 
All the db structure is based on SQLAlchemy package. The definition for each individual table is in app/models folder.
We will be using a PostgreSQL db, but for local testing all of this should work with SQLite.

### Data decisions
* Table names are singular, and table names and fields are defined with underscore casing, everything is in lower case 
(even acronyms) and the words are separated by underscores (some_class, some_func, some_var, etc).
* We only store images that can be defined as T1, DWI, rsfMRI or FieldMap. If the image type is different than those
we will ignore the image. 
* Image/visit relationship. When there is a 1-1 relationship between visit and image, we will respect that relation in the db
structure, when that is not the case and images and visit happened in different days, we will relate each image to the 
closest visit from the **past** (ignore visits that happened after the image).
* If there were more visits than images, we would still want to add all visits to the db, even if images were not acquired
on those visits (check `load_db/OASIS/load_visit.py` as a reference).


### Datasets structure
We assume that each dataset will have a set of metadata files. The idea is to store those files inside a folder located 
in `data/<datasetname>`. We also assume that each dataset will have a specific logic in order to load the metadata into
the db. We suggest to create those files in the directory `load_db/<dataset_name>` and to have each file starting with
the word `load_` followed by the name of the table (or main structure) that is going to be filled by that file, for example
`load_db/openpain/load_subject.py`. 

### Order to run the loading scripts
1) Run all scripts located into `load_db/seed`. These files create the necessary and common structure needed by all datasets.
2) Inside each dataset, for example OASIS, run the script to load the subjects: `load_db/OASIS/load_subject.py`
3) Run all other scripts from the dataset that start with `load_`, it doesn't matter the order.


### Master seed files
The idea is to have the master seed files in `data/seed_files` and download the most 
updated version of those spreadsheets **before** filling a new database and runnning the seed scripts (located in `load_db/seed`). 
Currently those files only cover the datasets names and the analyzed conditions. The other existing seed script (defined in folder
`load_db/seed/`) is for filling the file_type, but the definition of possible files is hard coded in the script (`load_
db/seed/load_file_type.py`).

### How to add a new dataset
1) Ensure that the dataset exists in the seed datasets_list.xls file
2) Upload dataset images to cloud storage, modifying the script `load_file/upload_datasets.py`
3) In the folder `data`, create a new folder for the dataset, and place the metadata files in there
4) In the folder `load_db`, create a new folder for the dataset, and create new scripts to load the metadata into the db,
number of scripts will depend on the type of metadata that we obtain from the dataset, but ideally we would want to have
a script to load subjects (`load_db/datasetname/load_subject.py`) and another to load images (`load_db/datasetname/load_image.py`)

### Medical diagnosis conditions
The master list of conditions is added as part of the seed files. Each datasets has it own mapping between the specific
conditions described in their metadata and our own list of conditions.
Conditions can be assigned to a Subject or to a Visit, when the condition is available for a Visit we prefer to store it
 at that levels as it's more specific of what is the diagnose for a specific image. If we have conditions per Visit we 
 decide to ignore Conditions per subject. In HCP (not AGING at least) we have all healthy subjects, and as there is no
 diagnose per visit (and this is a general condition of the subject), we added the condition id to the subject, to have 
 a more transparent structure. 
  
Most of the datasets have two scripts where it will upload conditions: `load_visit.py` (which creates all visits, without caring 
if they contain images or not) and `load_images.py` which loads the visits related to the images. In order to avoid repeated
code and more complexity for it's maintenance we recommend to store a function for mapping datasets diagnose conditions 
to the master list in the `__init__.py` file of each dataset, for example method `get_oasis_dx` in `load_db/OASIS/__init__.py`.

### Requirements:
Is necessary to have a config file inside the config folder and a `global.py` that reference the name of that file with 
the key ENVIRONMENT, example of `global.py`:
``
ENVIRONMENT = 'development'
``
and example of the file referenced by globals (in this case, `development.py`):
``
SQLALCHEMY_DATABASE_URI = 'sqlite:///xls.db'
SQLALCHEMY_TRACK_MODIFICATIONS = False
BUCKET = "bucket_name"
COS_CREDENTIALS = {
  "apikey": "apikey",
  "endpoints": "https://endpoint",
  "iam_apikey_description": "Description",
  "iam_apikey_name": "apikey_name",
  "iam_role_crn": "crn:example",
  "iam_serviceid_crn": "crn:v1:example",
  "resource_instance_id": "crn:v1:example"
}
AUTH_ENDPOINT = 'https://auth/token'
SERVICE_ENDPOINT = 'https://s3.endpoint'
``
### Dependencies
All the required packages are specified in the file requirements.txt. That file can be run using pip, with the command
`pip install -r requirements.txt` from the root of the repository. 
