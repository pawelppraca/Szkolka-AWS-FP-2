from boto3.session import Session
import boto3

#ACCESS_KEY = ''
#SECRET_KEY = ''

S3Bucket = 'adamm-bucket'


session = Session()
s3 = session.resource('s3')
your_bucket = s3.Bucket(S3Bucket)

#wildcard of file you search for
fileWildcard = "noquote.csv"
#folder on s3 where your search for file
S3FileFolder = 'simplemaps/files/'
#local folder to drop files
localFolder="D:/Docs/documents/python/csv/simplemaps_uscities_basicv1.79/"

for s3_file in your_bucket.objects.filter(Prefix=S3FileFolder):
    if s3_file.key.find(fileWildcard)!=-1 :# returns only file which contains string defined in fileWilcard
        pathfile = localFolder+s3_file.key #set path: folder + file
        your_bucket.download_file(Key=s3_file.key,Filename=pathfile.replace("simplemaps/files","")) # remove part of s3 folder (Filename) and download file
    else:
        pass




