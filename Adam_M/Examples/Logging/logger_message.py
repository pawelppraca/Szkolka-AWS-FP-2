import logging
MSG_FORMAT = '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s'
DATETIME_FORMAT = '%H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT, level=logging.DEBUG)

#logging.info("Test")

logger = logging.getLogger()
logger.info("Test")

"""logger = glueContext.get_logger()
logger.info("Test")"""