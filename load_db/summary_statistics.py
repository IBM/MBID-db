import pandas as pd
import matplotlib.pyplot as plt

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from app import create_app, db
from app.models import Image, Subject, SourceDataset, PreprocessTaskFile
from app.models.subject import GenderEnum
from config.globals import ENVIRONMENT
import seaborn as sns
from pathlib import Path

sns.set()

if __name__ == '__main__':
    config = os.environ
    app = create_app(os.environ.get('FLASK_CONFIG', ENVIRONMENT))
    with app.app_context():
        t1_images = db.session.query(Image,SourceDataset).join(SourceDataset).filter(Image.type == 'T1')
        t1_images_df = pd.read_sql(t1_images.statement, t1_images.session.bind)
        subjects = Subject.query
        subjects_df = pd.read_sql(subjects.statement, subjects.session.bind)
        preprocess_task_file = PreprocessTaskFile.query
        preprocess_task_file_df = pd.read_sql(preprocess_task_file.statement, preprocess_task_file.session.bind)
        preprocess_task_file_df['r_correlation'] = preprocess_task_file_df['preprocess_check_json'].apply(lambda x: x['r'])
        preprocessed_t1 = len(t1_images_df[t1_images_df.preprocessed.notna()])
        preprocessed_t1_subjects = len(t1_images_df[t1_images_df.preprocessed.notna()].subject_id.unique())
        total_t1 = len(t1_images_df)
        print('Total number of T1 images: ' + str(total_t1))
        print('T1 images preprocessed: '+str(preprocessed_t1))
        print('T1 % of preprocessed images: '+str(round((preprocessed_t1/total_t1)*100,0))+'%')
        print('Total number of subjects: '+str(len(subjects_df)))
        print('T1 processed images number of unique subjects: ' + str(preprocessed_t1_subjects))
        subjects_df['gender'] = subjects_df['gender'].map({GenderEnum.female: 'female', GenderEnum.male: 'male'})
        subjects_df['gender'] = subjects_df['gender'].astype('category')

###############################################################################
do_save=False
fig_dir = Path('/Users/pipolose/Desktop/local_work/figures')

fig, ax = plt.subplots()
sns.histplot(x='age_at_baseline',data=subjects_df, ax=ax, binwidth=3, kde=True, hue='gender')
ax.set_xlabel('Age')
ax.set_ylabel('Subjects')
ax.set_title('Age distribution' )
lg = ax.get_children()[-2]
lg.set_title('Sex')
for lt in lg.get_texts():
    lt.set_text(lt.get_text().capitalize())
plt.show()
if do_save:
    out_fn = fig_dir / 'age_distribution.pdf'
    fig.savefig(out_fn, dpi=300, bbox_inches='tight')

fig, ax = plt.subplots()
sns.countplot(x='gender',data=subjects_df, ax=ax)
ax.set_xlabel('Sex')
ax.set_ylabel('Subjects')
ax.set_title('Sex distribution')
ax.set_xticklabels(['Female', 'Male'])
ax.set_xlim([-.75, 1.75])
ax.set_ylim([0, 30000])
plt.show()
if do_save:
    out_fn = fig_dir / 'sex_distribution.pdf'
    fig.savefig(out_fn, dpi=300, bbox_inches='tight')

ds_counts = t1_images_df.groupby(['designation'])['id'].count()
fig, ax = plt.subplots()
ax = ds_counts.plot.bar(ax=ax)
ax.set_ylabel('Preprocessed Images')
ax.set_xlabel('')
xl = ax.get_xticklabels()
xl[0] = 'HCP'
ax.set_xticklabels(xl, rotation = 0)
ax.set_title('Images per study')
plt.show()
plt.show()
if do_save:
    out_fn = fig_dir / 'study_count.pdf'
    fig.savefig(out_fn, dpi=300, bbox_inches='tight')

fig, ax = plt.subplots()
sns.histplot(x='r_correlation', data=preprocess_task_file_df, ax=ax, kde=True)
ax.set_xlabel('Pearson\'s R')
ax.set_title('QA Metric: voxelwise correlation to MNI Brain Template')
plt.show()
if do_save:
    out_fn = fig_dir / 'pearson_dist.pdf'
    fig.savefig(out_fn, dpi=300, bbox_inches='tight')
