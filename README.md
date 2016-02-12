# MSSQL_Handler
python logging handler that writes to MSSQL database
Based on Vinay Sajip's DBHandler class (http://www.red-dove.com/python_logging.html)
    Adapted from https://gist.github.com/ykessler/2662203

    This version sacrifices performance for thread-safety:
    Instead of using a persistent cursor, we open/close connections for each entry.
