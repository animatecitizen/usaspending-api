from django.conf import settings
from django.db import connection

# NOTE
# The timeout values here only restrict the roles from application code. The actual timeout set in the DB could
# be a different value.
# Timeout set in DB on 27/03/2018 is 50s.

DEFAULT_DB_TIMEOUT_IN_MS = 1000

default_conn = connection.settings_dict['NAME']


DB_IDENTIFIER_AND_CONNECTION_TO_TIMEOUT_MAP = {
    settings.PROD_APP_DB_TIMEOUT_IDENTIFIER: {
        default_conn: 1000
    },
    settings.DEFAULT_DB_TIMEOUT_IDENTIFIER: {
        'default': 1000
    },
}