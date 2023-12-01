class CustomLogFilter(logging.Filter):
    def __init__(self, prefix):
        self.prefix = prefix

    def filter(self, record):
        # Only allow log records that start with the specified prefix
        return record.getMessage().startswith(self.prefix)
