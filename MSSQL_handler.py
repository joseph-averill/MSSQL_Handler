import pyodbc
import logging
import time


class MSSQLHandler(logging.Handler):
    """
    Logging handler for MSSQL.
    Based on Vinay Sajip's DBHandler class (http://www.red-dove.com/python_logging.html)
    Adapted from https://gist.github.com/ykessler/2662203

    This version sacrifices performance for thread-safety:
    Instead of using a persistent cursor, we open/close connections for each entry.

    """

    def __init__(self, server='localhost', db='app.db', table='log', user='', password=''):

        logging.Handler.__init__(self)
        self.table = table
        self.initial_sql = """
IF NOT EXISTS( SELECT * FROM INFORMATION_SCHEMA.Tables where TABLE_NAME = '{actualtable}' and TABLE_SCHEMA = 'dbo')
CREATE TABLE dbo.{actualtable}
(
    rowid bigint identity(1,1) not null primary key,
    entryDate datetime not null default (getdate()),
    Name nvarchar(200),
    LogLevel varchar(50),
    LogLevelName nvarchar(200),
    LogMessage varchar(max),
    Args varchar(max),
    Module varchar(200),
    FuncName varchar(200),
    LineNumber integer,
    Exception varchar(max),
    Process integer,
    Thread varchar(1000),
    ThreadName varchar(200)
)"""
        self.initial_sql = self.initial_sql.format(actualtable=table)

   #     print('initial sql is: ' + self.initial_sql)
        self.insertion_sql = """
INSERT INTO {actualtable}
(
    Name,
    LogLevel,
    LogLevelName,
    LogMessage,
    Args,
    Module,
    FuncName,
    LineNumber,
    Exception,
    Process,
    Thread,
    ThreadName
)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
"""
        self.insertion_sql = self.insertion_sql.format(actualtable=table)
   #     print('insertion sql is: ' + self.insertion_sql)
        # Create table if needed:
        self.connection_string = 'DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db + ';UID=' + user + ';PWD=' + password
        conn = pyodbc.connect(self.connection_string)
        cursor = conn.cursor()
        cursor.execute(self.initial_sql)
        conn.commit()
        conn.close()

    def formatDBTime(self, record):
        record.dbtime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(record.created))

    def emit(self, record):

        # Use default formatting:
        self.format(record)
        # Set the database time up:
        self.formatDBTime(record)
        if record.exc_info:
            record.exc_text = logging._defaultFormatter.formatException(record.exc_info)
        else:
            record.exc_text = ""
        # Insert log record:
        sql = self.insertion_sql
        conn = pyodbc.connect(self.connection_string)
        cursor = conn.cursor()
        cursor.execute(sql, [record.name, record.__dict__['levelno'], record.__dict__['levelname'],
                             record.__dict__['message'],
                             str(record.__dict__['args']), record.__dict__['module'], record.__dict__['funcName'],
                             record.__dict__['lineno'],
                             record.__dict__['exc_text'], record.__dict__['process'], record.__dict__['thread'],
                             record.__dict__['threadName']])
        conn.commit()
        conn.close()


def test():
    lh = MSSQLHandler(server='my_server', db='my_db', table='log')
    logger = logging.getLogger('test')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(lh)
    logger.info('test1')
    for i in range(3):
        logger.info('remaining %d seconds', i)
        time.sleep(1)
    logger.info('test2')


if __name__ == '__main__':
    test()
