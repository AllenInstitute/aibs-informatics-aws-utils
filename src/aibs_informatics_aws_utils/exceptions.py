# --------------------------------------------------------------------------
# DynamoDB Exceptions
# --------------------------------------------------------------------------


class DBException(Exception):
    """Generic Exception for any DynamoDB related Errors"""


class DBWriteException(DBException):
    """Generic Exception for DynamoDB *Put/Update/Delete* related Errors"""


class DBReadException(DBException):
    """Generic Exception for DynamoDB *Read Invocation* related Errors"""


class DBQueryException(DBReadException):
    """Generic Exception for DynamoDB *Query Invocation* related Errors"""


class DBQueryResultException(DBReadException):
    """Generic Exception for DynamoDB *Query Result* related Errors"""


class NonUniqueQueryResultException(DBQueryResultException):
    """Raised when 1 query result is expected but more are fewer are returned"""


class EmptyQueryResultException(DBQueryResultException):
    """Raised when at least 1 query result was expected but none were returned"""


# --------------------------------------------------------------------------
# Other AWS Exceptions
# --------------------------------------------------------------------------


class AWSError(ValueError):
    """AWS related Exception"""


class InvalidAmazonResourceNameError(AWSError):
    """Raised when ARN is invalid"""


class ResourceNotFoundError(AWSError):
    """Raised when AWS resource not found"""
