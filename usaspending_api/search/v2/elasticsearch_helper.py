import logging
import re
from django.conf import settings

from usaspending_api.awards.v2.lookups.elasticsearch_lookups import KEYWORD_DATATYPE_FIELDS
from usaspending_api.awards.v2.lookups.elasticsearch_lookups import indices_to_award_types
from usaspending_api.awards.v2.lookups.elasticsearch_lookups import TRANSACTIONS_LOOKUP
from usaspending_api.awards.v2.lookups.elasticsearch_lookups import AWARD_QUERY_TO_ES

from usaspending_api.core.elasticsearch.client import es_client_query
from elasticsearch import Elasticsearch
from pprint import pprint
from collections import defaultdict
import math
import datetime
import json


logger = logging.getLogger('console')

TRANSACTIONS_INDEX_ROOT = settings.TRANSACTIONS_INDEX_ROOT
DOWNLOAD_QUERY_SIZE = settings.DOWNLOAD_QUERY_SIZE
KEYWORD_DATATYPE_FIELDS = ['{}.raw'.format(i) for i in KEYWORD_DATATYPE_FIELDS]

TRANSACTIONS_LOOKUP.update({v: k for k, v in TRANSACTIONS_LOOKUP.items()})

CLIENT = Elasticsearch(settings.ES_HOSTNAME)


def preprocess(keyword):
    """Remove Lucene special characters instead of escaping for now"""
    processed_string = re.sub('[\/:\]\[\^!]', '', keyword)
    if len(processed_string) != len(keyword):
        msg = 'Stripped characters from ES keyword search string New: \'{}\' Original: \'{}\''
        logger.info(msg.format(processed_string, keyword))
        keyword = processed_string
    return keyword


def swap_keys(dictionary_):
    return dict((TRANSACTIONS_LOOKUP.get(old_key, old_key), new_key)
                for (old_key, new_key) in dictionary_.items())


def format_for_frontend(response):
    '''calls reverse key from TRANSACTIONS_LOOKUP '''
    response = [result['_source'] for result in response]
    return [swap_keys(result) for result in response]


def base_query(keyword, fields=KEYWORD_DATATYPE_FIELDS):
    keyword = preprocess(keyword)
    query = {
            "dis_max": {
                "queries": [{
                  'query_string': {
                        'query': keyword
                        }
                    },
                    {
                      "query_string": {
                            "query": keyword,
                            "fields": fields
                        }
                    }
                ]
            }
        }
    return query


def search_transactions(request_data, lower_limit, limit):
    '''
    filters: dictionary
    fields: list
    sort: string
    order: string
    lower_limit: integer
    limit: integer

    if transaction_type_code not found, return results for contracts
    '''
    keyword = request_data['keyword']
    query_fields = [TRANSACTIONS_LOOKUP[i] for i in request_data['fields']]
    query_fields.extend(['award_id'])
    query_sort = TRANSACTIONS_LOOKUP[request_data['sort']]
    query = {
        '_source': query_fields,
        'from': lower_limit,
        'size': limit,
        'query': base_query(keyword),
        'sort': [{
            query_sort: {
                'order': request_data['order']}
        }]
    }

    for index, award_types in indices_to_award_types.items():
        if sorted(award_types) == sorted(request_data['award_type_codes']):
            index_name = '{}-{}*'.format(TRANSACTIONS_INDEX_ROOT, index)
            break
    else:
        logger.exception('Bad/Missing Award Types. Did not meet 100% of a category\'s types')
        return False, 'Bad/Missing Award Types requested', None

    response = es_client_query(index=index_name, body=query, retries=10)
    if response:
        total = response['hits']['total']
        results = format_for_frontend(response['hits']['hits'])
        return True, results, total
    else:
        return False, 'There was an error connecting to the ElasticSearch cluster', None


def get_total_results(keyword, sub_index, retries=3):
    index_name = '{}-{}*'.format(TRANSACTIONS_INDEX_ROOT, sub_index.replace('_', ''))
    query = {'query': base_query(keyword)}

    response = es_client_query(index=index_name, body=query, retries=retries)
    if response:
        try:
            return response['hits']['total']
        except KeyError:
            logger.error('Unexpected Response')
    else:
        logger.error('No Response')
        return None


def spending_by_transaction_count(request_data):
    keyword = request_data['keyword']
    response = {}

    for category in indices_to_award_types.keys():
        total = get_total_results(keyword, category)
        if total is not None:
            if category == 'directpayments':
                category = 'direct_payments'
            response[category] = total
        else:
            return total
    return response


def get_sum_aggregation_results(keyword, field='transaction_amount'):
    """
    Size has to be zero here because you only want the aggregations
    """
    index_name = '{}-*'.format(TRANSACTIONS_INDEX_ROOT)
    query = {
        'query': base_query(keyword),
        'aggs': {
            'transaction_sum': {
                'sum': {
                    'field': field
                }
            }
        }
    }

    response = es_client_query(index=index_name, body=query, retries=10)
    if response:
        return response['aggregations']
    else:
        return None


def spending_by_transaction_sum(filters):
    keyword = filters['keyword']
    return get_sum_aggregation_results(keyword)


def get_download_ids(keyword, field, size=10000):
    '''
    returns a generator that
    yields list of transaction ids in chunksize SIZE

    Note: this only works for fields in ES of integer type.
    '''
    index_name = '{}-*'.format(TRANSACTIONS_INDEX_ROOT)
    n_iter = DOWNLOAD_QUERY_SIZE // size

    max_iterations = 10
    total = get_total_results(keyword, '*', max_iterations)
    if total is None:
        logger.error('Error retrieving total results. Max number of attempts reached')
        return
    required_iter = (total // size) + 1
    n_iter = min(max(1, required_iter), n_iter)
    for i in range(n_iter):
        query = {
            "_source": [field],
            "query": base_query(keyword),
            "aggs": {
                "results": {
                    "terms": {
                        "field": field,
                        "include": {
                            "partition": i,
                            "num_partitions": n_iter
                        },
                        "size": size
                    }
                }
            },
            "size": 0
        }

        response = es_client_query(index=index_name, body=query, retries=max_iterations, timeout='3m')
        if not response:
            raise Exception('Breaking generator, unable to reach cluster')
        results = []
        for result in response['aggregations']['results']['buckets']:
            results.append(result['key'])
        yield results


def get_sum_and_count_aggregation_results(keyword):
    index_name = '{}-*'.format(TRANSACTIONS_INDEX_ROOT)
    query = {
        "query": base_query(keyword),
        "aggs": {
            "prime_awards_obligation_amount": {
                "sum": {
                    "field": "transaction_amount"
                }
            },
            "prime_awards_count": {
                "value_count": {
                    "field": "transaction_id"
                }
            }
        },
        "size": 0}
    response = es_client_query(index=index_name, body=query, retries=10)
    if response:
        try:
            results = {}
            results["prime_awards_count"] = response['aggregations']["prime_awards_count"]["value"]
            results["prime_awards_obligation_amount"] = \
                round(response['aggregations']["prime_awards_obligation_amount"]["value"], 2)
            return results
        except KeyError:
            logger.error('Unexpected Response')
    else:
        return None


def spending_by_transaction_sum_and_count(request_data):
    return get_sum_and_count_aggregation_results(request_data['keyword'])


###################################
####                      #########
####     THUNDER DOME     #########
####                      #########
###################################


AWARDS_INDEX = 'awards-temp*'
ES_FILTER_FIELDS = ['recipient_location', 'pop']
RANGE_QUERIES = ['action_date', 'transaction_amount']
BAD_KEYS = ['psc_codes','keyword'] # don't have these in ES


AWARD_QUERY_TO_ES.update({v: k for k, v in AWARD_QUERY_TO_ES.items()})


def swap_keys2(dictionary_):
    return dict((AWARD_QUERY_TO_ES.get(old_key, old_key), new_key)
                for (old_key, new_key) in dictionary_.items())


def range_query(filters, column):
    filter_ = filters[column]
    queries = []
    for f in filter_:
        start = {'range': {column: {'gte': min(f.values())}}}
        end = {'range': {column: {'lte': max(f.values())}}}
        if len(filter_) == 1:
            print('range 1')
            # queries.extend([start, end])
            return {'bool': {'must': [start, end]}}
        else:
            queries.append((start, end))
    return format_dismax(queries, format_=False)


def location_helpers_es(filters, type_='place_of_performance_locations',
                        type='match'):
    '''returns a must filter
    also returns a dismax if the
    length is > 1 to get all combinations
    '''
    filter_ = filters[type_]
    new_filters = []
    for f in filter_:
        temp_filter = []
        temp_dict = {}
        for k, v in f.items():
            # this value is hard coded which is why the function is special
            es_value = '{}_{}_code'.format(type_, k)
            temp_filter.append({es_value: v})
        temp_dict = reduce(lambda a, b: a.update(b) or a, temp_filter)
        new_filters.append(temp_dict)
    if len(new_filters) > 1:
        return format_dismax(new_filters, query_type='match')
    else:
        return match_query(new_filters, query_type='match')


def format_dismax(list_of_queries, query_type='terms', format_=True):
    query_list = []
    for query in list_of_queries:
        if format_:
            data = match_query(query, query_type)
            query_list.append(dict(bool=dict(must=data)))
        else:
            query_list.append(dict(bool=dict(filter=query)))
    response = dict(dis_max=dict(queries=query_list))
    return response


def format_filters_query(filters):
    '''
    First step in filters manipulation, pops fields
    that must be formated for dis_max must query

    this function handles multiple depths of queries
    for different intervals for times, amounts, and different
    location combinations.
    '''

    es_filter_query = []

    for field in ES_FILTER_FIELDS:
        temp_filters = []
        data = filters.get(field, None)
        if data:
            temp_filters.append(location_helpers_es(filters, field))
            filters.pop(field)
            es_filter_query.append(temp_filters)

    for field in RANGE_QUERIES:
        temp_filters = []
        data = filters.get(field, None)
        if data:
            temp_filters.append(range_query(filters, field))
            filters.pop(field)
            es_filter_query.append(temp_filters)
    return es_filter_query, filters


def match_query(filters, query_type='terms'):
    '''
    This formats the rest of the filters in the request
    to a match query and defaults to terms
    becuase the expected type for each value is a list.

    this is called with list type for some of
    the dismax queries and filter queries, in whcih type
    we return a boolean query that will be used to filter
    '''
    must_query = []
    if type(filters) is list:
        for item in filters:
            for column, values in item.items():
                must_query.append({query_type: {column: values}})
        return dict(bool=dict(must=must_query))
    else:
        for column, values in filters.items():
            must_query.append({query_type: {column: values}})
    return must_query


def preprocess(filters, type_='agencies'):
    '''reformats agency filter to be a
    list like all of the others
    '''
    print('called latest preprocess')
    if filters.get(type_, None):
        filter_ = filters.pop(type_)
        response = defaultdict(list)
        for f in filter_:
            # this value is hard coded so this is also special
            es_value = "{}_{}_agency_name".format(f['type'], f['tier'])
            response[es_value].append(f['name'])
        filters.update(dict(response))
    for f in BAD_KEYS:
        filters.pop(f, None)
    return swap_keys2(filters)


def time_aggs(group, aggregation_field='transaction_amount'):
    if group.startswith('fiscal'):
        group = 'year'
    time_period = dict(date_histogram=dict(field='action_date',
                                           interval=group))
    time_period['aggs'] = dict(aggregated_amount=dict(sum=dict(field=aggregation_field)))
    return dict(results=time_period)


def geography_aggs(filters, aggregation_field='transaction_amount'):
    
    scope = filters['scope']
    layer = filters['geo_layer']
    if scope.startswith('place_of_performance'):
        scope = 'pop'
    field = '{}_{}_code'.format(scope, layer)
    if layer.startswith('zip'):
        field = '{}_{}'.format(scope, layer)

    aggregated_amount = dict(sum=dict(field=aggregation_field))
    shape_code = dict(terms=dict(field=field),
                      aggs=dict(aggregated_amount=aggregated_amount)
                      )
    return dict(results=shape_code)


def master_filter_function(filters):
    '''returns the filter query
    format_filters_query MUST be called first becasue it pops keys not to be
    put in the match query

    the format_filters_query handles multiple depths of queries
    for different intervals for times, amounts, and different
    location combinations

    query = {"bool": {
                 "must": es_must_query,
                 "filter": es_filter_query
                }
            }
    '''
    filters = preprocess(filters)
    es_filter_query, filters = format_filters_query(filters)
    es_must_query = match_query(filters)
    filters_portion = dict(bool=dict(filter=es_filter_query,
                                     must=es_must_query))
    return filters_portion


def spending_over_time(request_data):
    '''
    the response['aggregations'] needs to be
    reformated and returned for the frontend
    '''
    filters_portion = master_filter_function(request_data['filters'])
    group = request_data['group']
    aggs_portion = time_aggs(group)
    print('got aggs')

    query = dict(query=filters_portion, aggs=aggs_portion)
    # pprint(query)
    try:
        response = CLIENT.search(AWARDS_INDEX, body=query)
    except Exception as e:
        print(str(e))
    print('\n\n')
    print('TOTAL RESPONSES >>>')
    print(response['hits']['total'])

    return format_time_frontend(response, group)


def format_time_frontend(response, group, divisor=1):
    '''this function formats a date from integer in the key
    and groups accordingly
    '''
    data = response['aggregations']['results']['buckets']
    results = []
    if group == 'quarter':
        divisor = 3
    for d in data:
        print(d)
        # {'key_as_string': '2017-10-01', 'key': 1506816000000, 'doc_count': 641, 'aggregated_amount': {'value': 30667969283.049995}})
        total_amount = d['aggregated_amount']['value']
        date = datetime.datetime.fromtimestamp(d['key'] / 1e3)
        group_value = int(math.ceil(date.month//divisor))
        response_data = {'fiscal_year': date.year,
                         group: group_value}
        data = dict(aggregated_amount=total_amount,
                    time_period=response_data)
        results.append(data)
    # pprint(results)
    return results


def spending_by_geography(request_data):
    '''
    the response['aggregations'] needs to be
    reformated and returned for the frontend
    '''
    filters_portion = master_filter_function(request_data['filters'])
    aggs_portion = geography_aggs(request_data)
    print('got aggs geography')
    query = dict(query=filters_portion, aggs=aggs_portion)
    # pprint(query)
    try:
        response = CLIENT.search(AWARDS_INDEX, body=json.dumps(query))
    except Exception as e:
        print(str(e))
    print('\n\n')
    print('TOTAL RESPONSES >>>')
    print(response['hits']['total'])
    return geography_frontend_response(response)


def geography_frontend_response(response):
    data = response['aggregations']['results']['buckets']
    results = []
    for d in data:
        total_amount = d['aggregated_amount']['value']
        key = d['key']
        # results.append({key: total_amount})
        data = dict(aggregated_amount=total_amount,
                    shape_code=key,
                    display_name=key)
        results.append(data)
    return results


def search_transactions2(request_data, lower_limit, limit):
    '''
    filters: dictionary
    fields: list
    sort: string
    order: string
    lower_limit: integer
    limit: integer

    if transaction_type_code not found, return results for contracts
    '''
    # keyword = request_data['keyword']
    print('got search transactions2')
    query_fields = [TRANSACTIONS_LOOKUP[i] for i in request_data['fields']]
    query_fields.extend(['award_id'])
    query_sort = TRANSACTIONS_LOOKUP[request_data['sort']]

    filter_query = master_filter_function(request_data.get('filters'))
    filter_query = dict(query=filter_query)
    pprint(filter_query)
    query = {
        '_source': query_fields,
        'from': lower_limit,
        'size': limit,
        'query': filter_query['query'],
        'sort': [{
            query_sort: {
                'order': request_data['order']}
        }]
    }

    try:
        response = CLIENT.search(AWARDS_INDEX, body=json.dumps(query))
    except Exception as e:
        print(str(e))
    if response:
        total = response['hits']['total']
        results = format_for_frontend(response['hits']['hits'])
        print('\n\n')
        print('TOTAL RESPONSES >>>')
        print(total)
        return True, results, total
    else:
        return False, 'There was an error connecting to the ElasticSearch cluster', None

