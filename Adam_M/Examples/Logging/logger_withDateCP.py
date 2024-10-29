import logging,time
from datetime import datetime

MSG_FORMAT = '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s'
DATETIME_FORMAT = '%H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT, level=logging.DEBUG)

#logger with date time checkpoints
logger = logging.getLogger()

#now = datetime.now(tz=timezone.UTC)
# 'YYYY-mm-ddTHH:MM:SSZ'
#answer = now.strftime('%FT%XZ')


a = datetime.now() # current date and time

af = a.strftime("%Y-%m-%d, %H:%M:%S")
#print("year:", year)
logger.info(af)


logger.info("Test")
time.sleep(3)

b = datetime.now()
bf = b.strftime("%Y-%m-%d, %H:%M:%S")
#print("year:", year)
logger.info(bf)

c = b-a 
logger.info('Difference: ' +str(c))
minutes = divmod(c.seconds, 60)
logger.info('Total: ' +str(minutes[0])+ ' minutes '+str(minutes[1]) +' seconds')


#https://www.programiz.com/python-programming/datetime/strftime