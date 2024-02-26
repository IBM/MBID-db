import pdb

def years_education_convertion(field845, field6138):
    if field845:
        try:
            field845 = int(field845)
            if field845 > 0:
                return field845 - 5
            elif field845 == -1 or field845 == -3:
                return None
            elif field845 == -2:
                return 0
        except:
            return None

    if field6138:
        try:
            field6138 = int(field6138)
            if field845 is None:
                if field6138 == 1:
                    return 17
                else:
                    return None
        except:
            return None
    else:
        return None
def get_file_list(cos_client, bucket_name, contains_key=None, starts_with=None):
    # Initialize result
    existingFileList = list()

    # Initialize the continuation token
    ContinuationToken = ''

    # Loop in case there are more than 1000 files
    done = False
    while not done:

        # Get the file list (include continuation token for paging)
        res = cos_client.list_objects_v2(Bucket=bucket_name, ContinuationToken=ContinuationToken)

        # Put the files in the set
        for r in res['Contents']:
            if contains_key and starts_with:
                if contains_key in r['Key'] and r['Key'].startswith(starts_with):
                    existingFileList.append(r)
            elif contains_key:
                if contains_key in r['Key']:
                    existingFileList.append(r)
            elif starts_with:
                if r['Key'].startswith(starts_with):
                    existingFileList.append(r)
            else:
                existingFileList.append(r)
        # Check if there are more files and grab the continuation token if true
        if res['IsTruncated'] == True:
            ContinuationToken = res['NextContinuationToken']
            done = False
        else:
            done = True
            break
    return existingFileList

def get_file_set(cos_client, bucket_name, contains_key=None, starts_with=None):
    # Initialize result
    existingFileSet = set()

    # Initialize the continuation token
    ContinuationToken = ''

    # Loop in case there are more than 1000 files
    done = False
    while not done:

        # Get the file list (include continuation token for paging)
        res = cos_client.list_objects_v2(Bucket=bucket_name, ContinuationToken=ContinuationToken)

        # Put the files in the set
        for r in res['Contents']:
            if contains_key and starts_with:
                if contains_key in r['Key'] and r['Key'].startswith(starts_with):
                    existingFileSet.add(r['Key'])
            elif contains_key:
                if contains_key in r['Key']:
                    existingFileSet.add(r['Key'])
            elif starts_with:
                if r['Key'].startswith(starts_with):
                    existingFileSet.add(r['Key'])
            else:
                existingFileSet.add(r['Key'])
        # Check if there are more files and grab the continuation token if true
        if res['IsTruncated'] == True:
            ContinuationToken = res['NextContinuationToken']
            done = False
        else:
            done = True
            break

    # Return the file set
    return (existingFileSet)

