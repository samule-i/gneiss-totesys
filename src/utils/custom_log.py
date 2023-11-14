import logging


def logger(name=None):
    log = logging.getLogger()
    log.setLevel(logging.INFO)
    log.handlers = []  # remove handler provided by AWS
    log.propagate = False

    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    log_fmt = logging.Formatter(
        "[%(levelname)s] %(message)s - %(module)s/%(funcName)s() - %(name)s"
    )
    handler.setFormatter(log_fmt)

    log.addHandler(handler)
    return log
