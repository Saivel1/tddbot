import logging
import sys


logger = logging.getLogger(__name__)
level = logging.DEBUG
logger.setLevel(level)  # <<— ВАЖНО: иначе DEBUG не увидишь
logger.propagate = False

formatter_1 = logging.Formatter(
    fmt='[%(asctime)s] #%(levelname)-8s %(filename)s:%(lineno)d - %(name)s:%(funcName)s - %(message)s'
)

# Консоль (хочешь именно stdout — укажи stream=sys.stdout)
stdout_handler = logging.StreamHandler(stream=sys.stdout)
stdout_handler.setLevel(level)
stdout_handler.setFormatter(formatter_1)
logger.addHandler(stdout_handler)