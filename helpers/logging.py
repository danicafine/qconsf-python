import logging

def set_logging(logger_name, logging_level=logging.WARN):
	logger = logging.getLogger(logger_name)
	logger.setLevel(logging_level)

	return logger