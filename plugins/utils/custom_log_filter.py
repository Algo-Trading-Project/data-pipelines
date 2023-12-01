import logging

class CustomLogFilter(logging.Filter):
    """
    A custom logging filter that selectively passes log records based on a predefined prefix.

    This filter is intended to be attached to a logging.Logger instance to control
    which log messages are allowed through the filter based on whether they start 
    with a specific prefix. This is particularly useful for distinguishing and 
    isolating logs generated from specific parts of an application.

    Attributes:
        prefix (str): The prefix used to filter log messages. Only messages that start 
                      with this prefix will pass the filter.
    """

    def __init__(self, prefix):
        """
        Initialize the CustomLogFilter with a specified prefix.

        Parameters:
            prefix (str): The prefix to filter log messages on. Log messages that start
                          with this prefix will be allowed through the filter, while
                          all others will be filtered out.
        """
        self.prefix = prefix

    def filter(self, record):
        """
        Determine if the specified log record should be allowed through the filter.

        This method checks if the log record's message starts with the filter's prefix. 
        If it does, the log record is allowed; otherwise, it is filtered out. This method 
        is automatically called by the logging framework and should not be called directly.

        Parameters:
            record (logging.LogRecord): The log record to be evaluated by the filter.

        Returns:
            bool: True if the log record's message starts with the specified prefix, 
                  allowing the record to be logged. False if the record does not start 
                  with the prefix, causing it to be filtered out.
        """
        return record.getMessage().startswith(self.prefix)
