import os
import shutil
import gzip
import pdb


def read_s3_contents(cos_client, bucket_name, key):
    response = cos_client.Object(bucket_name, key).get()
    return response['Body'].read()
def upload_directory(directory_upload, dataset_name, cos_client, bucket):
    try:
        cos_client.head_object(Bucket=bucket, Key=dataset_name)
    except:
        cos_client.put_object(Bucket=bucket, Key=(dataset_name + '/'))
    for subdir, directories, files in os.walk(directory_upload):
        for filename in files:
            # Ignore hidden files
            subdir_split = subdir.split(dataset_name)[1]+ "/"
            #if not len(subdir_split) == 0:
            #    subdir_split = subdir_split + "/"
            if not filename.startswith('.') and (not filename.endswith('tar.gz') and not filename.endswith('zip')):
                if filename.endswith('nii'):
                    with open(subdir+ "/" + filename, 'rb') as f_in:
                        with gzip.open(subdir+ "/" + filename + '.gz', 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    filename = filename + '.gz'
                    cos_client.upload_file(subdir + "/" + filename, bucket, dataset_name +
                                           subdir_split + filename)
                    print (filename)
                else:
                    cos_client.upload_file(subdir+ "/" + filename, bucket, dataset_name+
                                           subdir_split+filename)
                    print (filename)

def size_count_files_disk(directory_upload):
    total_size = 0
    total_count = 0
    for subdir, directories, files in os.walk(directory_upload):
        for filename in files:
            if not filename.startswith('.') \
                and (not filename.endswith('tar.gz') and
                     not filename.endswith('zip')):
                if not filename.endswith('nii'):
                    total_count += 1
                    total_size += os.path.getsize(subdir+ "/" + filename)
    print('Files size in disk: '+str(total_size))
    print('Files count in disk: '+str(total_count))
    return total_size, total_count


def size_count_files_bucket(bucket, prefix):
    total_size = 0
    total_count = 0
    for i in bucket.objects.all():
        if i.key.startswith(prefix) and i.size!=0:
            total_size += i.size
            total_count += 1
    print('Files size in bucket: '+str(total_size))
    print('Files count in bucket: '+str(total_count))
    return total_size, total_count

def compare_dataset_bucket_disk(dataset_dir, bucket, multidir_prefix=False):
    if multidir_prefix == True:
        dataset_prefix = '/'.join(dataset_dir.split('/')[-2:])
    else:
        dataset_prefix = dataset_dir.split('/')[-1]
    disk_files_size, disk_files_count = size_count_files_disk(dataset_dir)
    bucket_files_size, bucket_files_count = size_count_files_bucket(bucket, dataset_prefix)
    if disk_files_size == bucket_files_size:
        print(('SUCCESS: Files in bucket and in disk have the same size for {0} dataset').format(dataset_prefix))
    else:
        print(('WARNING: Files in bucket and in disk have a different size for {0} dataset').format(dataset_prefix))
    if disk_files_count == bucket_files_count:
        print(('SUCCESS: The count of files in bucket and in disk is the same for {0} dataset').format(dataset_prefix))
    else:
        print(('WARNING: The count of files in bucket and in disk is different for {0} dataset').format(dataset_prefix))
