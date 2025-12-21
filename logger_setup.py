import logging
import sys
from config import settings as s

time_format = '%d-%m %H:%M:%S'

logger = logging.getLogger(__name__)
if s.DEBUG:
    level = logging.DEBUG
else:
    level = logging.INFO

    
logger.setLevel(level)
logger.propagate = False

formatter_1 = logging.Formatter(
    fmt='[%(asctime)s] #%(levelname)-4s %(filename)s:%(lineno)d - %(name)s:%(funcName)s - %(message)s',
    datefmt=time_format
)

# Консоль (хочешь именно stdout — укажи stream=sys.stdout)
stdout_handler = logging.StreamHandler(stream=sys.stdout)
stdout_handler.setLevel(level)
stdout_handler.setFormatter(formatter_1)
logger.addHandler(stdout_handler)