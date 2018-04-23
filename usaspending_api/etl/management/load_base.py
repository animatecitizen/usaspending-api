"""
Code for loaders in management/commands to inherit from or share.
"""

import logging
import time
from datetime import datetime
from decimal import Decimal

import dateutil
from copy import copy
from django import db
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import connections
from django.core.cache import caches

from usaspending_api.awards.models import Award
from usaspending_api.awards.models import TransactionNormalized, TransactionFABS, TransactionFPDS
from usaspending_api.etl.broker_etl_helpers import PhonyCursor, setup_broker_fdw
from usaspending_api.etl.helpers import update_model_description_fields
from usaspending_api.references.helpers import canonicalize_location_dict
from usaspending_api.references.models import (Agency, LegalEntity, Cfda, Location, )
from usaspending_api.references.abbreviations import territory_country_codes

# Lists to store for update_awards and update_contract_awards
award_update_id_list = []
award_contract_update_id_list = []

awards_cache = caches['awards']
logger = logging.getLogger('console')


class Command(BaseCommand):
    """
    This command will load a single submission from the DATA Act broker. If
    we've already loaded the specified broker submission, this command
    will remove the existing records before loading them again.
    """
    help = "Loads a single submission from the DATA Act broker." \
           "The DATA_BROKER_DATABASE_URL environment variable must set so we can pull submission data from their db."

    def add_arguments(self, parser):

        parser.add_argument(
            '--test',
            action='store_true',
            dest='test',
            default=False,
            help='Runs the submission loader in test mode and uses stored data rather than pulling from a database'
        )
        parser.add_argument(
            '--noclean',
            action='store_true',
            dest='noclean',
            default=False,
            help='Skips the cleanup step to update model description fields'
        )

    def handle(self, *args, **options):
        awards_cache.clear()

        # Grab the data broker database connections
        if not options['test']:
            try:
                db_conn = connections['data_broker']
                db_cursor = db_conn.cursor()
            except Exception as err:
                logger.critical('Could not connect to database. Is DATA_BROKER_DATABASE_URL set?')
                logger.critical(print(err))
                return
        else:
            db_cursor = PhonyCursor()

        self.handle_loading(db_cursor=db_cursor, *args, **options)

        if not options['noclean']:
            # TODO: If this is slow, add ID limiting as below
            logger.info('Updating model description fields...')
            update_model_description_fields()

        # TODO: Find out where this is being used
        # logger.info('Updating awards to reflect their latest associated transaction info...')
        # update_awards(tuple(award_update_id_list))
        #
        # logger.info('Updating contract-specific awards to reflect their latest transaction info...')
        # update_contract_awards(tuple(award_contract_update_id_list))
        #
        # logger.info('Updating award category variables...')
        # update_award_categories(tuple(award_update_id_list))

        # Done!
        logger.info('FINISHED')


def run_sql_file(file_path, parameters):
    with db.connection.cursor() as cursor:
        with open(file_path) as infile:
            for raw_sql in infile.read().split('\n\n\n'):
                if raw_sql.strip():
                    sql_start_time = time.time()
                    cursor.execute(raw_sql, parameters)
                    some_sql = "\n".join(raw_sql.splitlines()[:3])
                    logger.info('sql:\n\n{}\n\ntime: {}\n\n'.format(some_sql, time.time() - sql_start_time))


def format_date(date_string):
    try:
        return dateutil.parser.parse(date_string).date()
    except (TypeError, ValueError):
        return None


def load_data_into_model(model_instance, data, **kwargs):
    """
    Loads data into a model instance
    Data should be a row, a dict of field -> value pairs
    Keyword args:
        field_map - A map of field columns to data columns. This is so you can map
                    a field in the data to a different field in the model. For instance,
                    model.tas_rendering_label = data['tas'] could be set up like this:
                    field_map = {'tas_rendering_label': 'tas'}
                    The algorithm checks for a value map before a field map, so if the
                    column is present in both value_map and field_map, value map takes
                    precedence over the other
        value_map - Want to force or override a value? Specify the field name for the
                    instance and the data you want to load. Example:
                    {'update_date': timezone.now()}
                    The algorithm checks for a value map before a field map, so if the
                    column is present in both value_map and field_map, value map takes
                    precedence over the other
        save - Defaults to False, but when set to true will save the model at the end
        reverse -   Field names matching this regex should be reversed
                    (multiplied by -1) before saving.
        as_dict - If true, returns the model as a dict instead of saving or altering
    """
    field_map = kwargs.get('field_map')
    value_map = kwargs.get('value_map')
    save = kwargs.get('save', False)
    as_dict = kwargs.get('as_dict', False)
    reverse = kwargs.get('reverse')

    # Grab all the field names from the meta class of the model instance
    fields = [field.name for field in model_instance._meta.get_fields()]
    mod = model_instance

    if as_dict:
        mod = {}

    for field in fields:
        # Let's handle the data source field here for all objects
        if field is 'data_source':
            store_value(mod, field, 'DBR', reverse)
        broker_field = field
        # If our field is the 'long form' field, we need to get what it maps to
        # in the broker so we can map the data properly
        if broker_field in settings.LONG_TO_TERSE_LABELS:
            broker_field = settings.LONG_TO_TERSE_LABELS[broker_field]
        sts = False
        if value_map:
            if broker_field in value_map:
                store_value(mod, field, value_map[broker_field], reverse)
                sts = True
            elif field in value_map:
                store_value(mod, field, value_map[field], reverse)
                sts = True
        if field_map and not sts:
            if broker_field in field_map:
                try:
                    store_value(mod, field, data[field_map[broker_field]], reverse)
                    sts = True
                except KeyError:
                    print('column {} missing from data'.format(field_map[broker_field]))
            elif field in field_map:
                store_value(mod, field, data[field_map[field]], reverse)
                sts = True
        if broker_field in data and not sts:
            store_value(mod, field, data[broker_field], reverse)
        elif field in data and not sts:
            store_value(mod, field, data[field], reverse)

    if save:
        model_instance.save()
    if as_dict:
        return mod
    else:
        return model_instance


def create_location(location_map, row, location_value_map=None):
    """
    Create a location object

    Input parameters:
        - location_map: a dictionary with key = field name on the location model and value = corresponding field name
          on the current row of data
        - row: the row of data currently being loaded
    """
    if location_value_map is None:
        location_value_map = {}

    row = canonicalize_location_dict(row)
    location_data = load_data_into_model(
        Location(), row, value_map=location_value_map, field_map=location_map, as_dict=True, save=False)

    return Location.objects.create(**location_data)


def get_or_create_location(location_map, row, location_value_map=None, empty_location=None, d_file=False, save=True):
    """
    Retrieve or create a location object

    Input parameters:
        - location_map: a dictionary with key = field name on the location model
            and value = corresponding field name on the current row of data
        - row: the row of data currently being loaded
    """
    if location_value_map is None:
        location_value_map = {}

    row = canonicalize_location_dict(row)

    # For only FABS
    if "place_of_performance_code" in row:
        # If the recipient's location country code is empty or it's 'UNITED STATES
        # OR the place of performance location country code is empty and the performance code isn't 00FORGN
        # OR the place of performance location country code is empty and there isn't a performance code
        # OR the country code is a US territory
        # THEN we can assume that the location country code is 'USA'
        if ('recipient_flag' in location_value_map and location_value_map['recipient_flag'] and
                (row[location_map.get('location_country_code')] is None or
                    row[location_map.get('location_country_code')] == 'UNITED STATES')) or \
                ('place_of_performance_flag' in location_value_map and
                    location_value_map['place_of_performance_flag'] and
                    row[location_map.get('location_country_code')] is None and
                    "performance_code" in location_map and row[location_map["performance_code"]] != '00FORGN') or \
                ('place_of_performance_flag' in location_value_map and
                    location_value_map['place_of_performance_flag'] and
                    row[location_map.get('location_country_code')] is None and
                    "performance_code" not in location_map) or \
                (row[location_map.get('location_country_code')] in territory_country_codes):
            row[location_map["location_country_code"]] = 'USA'

    state_code = row.get(location_map.get('state_code'))
    if state_code is not None:
        # Remove . in state names (i.e. D.C.)
        location_value_map.update({'state_code': state_code.replace('.', '')})

    location_value_map.update({
        'location_country_code': location_map.get('location_country_code'),
        'country_name': location_map.get('location_country_name'),
        'state_code': None,  # expired
        'state_name': None,
    })

    location_data = load_data_into_model(
        Location(), row, value_map=location_value_map, field_map=location_map, as_dict=True)

    del location_data['data_source']  # hacky way to ensure we don't create a series of empty location records
    if len(location_data):

        if len(location_data) == 1 and "place_of_performance_flag" in location_data and\
                location_data["place_of_performance_flag"]:
            location_object = None
            created = False
        elif save:
            location_object = load_data_into_model(Location(), row, value_map=location_value_map,
                                                   field_map=location_map, as_dict=False, save=True)
            created = False
        else:
            location_object = load_data_into_model(Location(), row, value_map=location_value_map,
                                                   field_map=location_map, as_dict=False)
            # location_object = Location.objects.create(**location_data)
            created = True

        return location_object, created
    else:
        # record had no location information at all
        return None, None


def store_value(model_instance_or_dict, field, value, reverse=None):
    if value is None:
        return
    if field.endswith('date'):  # turn datetimes into dates
        if isinstance(value, str):
            try:
                value = dateutil.parser.parse(value).date()
            except (TypeError, ValueError):
                pass
    if reverse and reverse.search(field):
        value = -1 * Decimal(value)
    if isinstance(model_instance_or_dict, dict):
        model_instance_or_dict[field] = value
    else:
        setattr(model_instance_or_dict, field, value)
