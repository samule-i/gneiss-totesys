import logging


def logger(name):
    log = logging.getLogger(name)
    log.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    log_fmt = logging.Formatter(
        '''%(levelname)s - %(message)s - %(name)s -
        %(module)s/%(funcName)s()''')
    handler.setFormatter(log_fmt)
    log.addHandler(handler)
    return log
