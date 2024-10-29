from boto3.session import Session
import boto3

#ACCESS_KEY = ''
#SECRET_KEY = ''


s3 = boto3.resource('s3')
S3Bucket = s3.Bucket('adamm-bucket')

#wildcard of file you search for
fileWildcard = ".csv" #file: .csv or folder: folderName
#folder on s3 where your search for file
S3FileFolder = 'simplemaps/files/'

for s3_file in S3Bucket.objects.filter(Prefix=S3FileFolder):
    if s3_file.key.find(fileWildcard)!=-1 :# returns only file which contains string defined in fileWildcard
        pathfile = s3_file.key #set path: folder + file
        s3_file.delete()
        print(f"file: {pathfile} has been deleted")




