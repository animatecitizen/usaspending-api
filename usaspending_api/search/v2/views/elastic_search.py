import ast
import logging

from collections import OrderedDict
from datetime import date
from fiscalyear import FiscalDate
from functools import total_ordering

from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.cache_decorator import cache_response
from django.db.models import Sum, Count, F, Value, FloatField
from django.db.models.functions import ExtractMonth, Cast, Coalesce

from usaspending_api.awards.models_matviews import UniversalAwardView
from usaspending_api.awards.models_matviews import UniversalTransactionView
from usaspending_api.awards.v2.filters.location_filter_geocode import geocode_filter_locations
from usaspending_api.awards.v2.filters.matview_filters import matview_search_filter
from usaspending_api.awards.v2.filters.filter_helpers import sum_transaction_amount
from usaspending_api.awards.v2.filters.view_selector import can_use_view
from usaspending_api.awards.v2.filters.view_selector import get_view_queryset
from usaspending_api.awards.v2.filters.view_selector import spending_by_award_count
from usaspending_api.awards.v2.filters.view_selector import spending_by_geography
from usaspending_api.awards.v2.filters.view_selector import spending_over_time
from usaspending_api.awards.v2.lookups.lookups import award_type_mapping
from usaspending_api.awards.v2.lookups.lookups import contract_type_mapping
from usaspending_api.awards.v2.lookups.lookups import loan_type_mapping
from usaspending_api.awards.v2.lookups.lookups import non_loan_assistance_type_mapping
from usaspending_api.awards.v2.lookups.matview_lookups import award_contracts_mapping
from usaspending_api.awards.v2.lookups.matview_lookups import loan_award_mapping
from usaspending_api.awards.v2.lookups.matview_lookups import non_loan_assistance_award_mapping
from usaspending_api.common.exceptions import ElasticsearchConnectionException
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers import generate_fiscal_month, get_simple_pagination_metadata
from usaspending_api.core.validator.award_filter import AWARD_FILTER
from usaspending_api.core.validator.pagination import PAGINATION
from usaspending_api.core.validator.tinyshield import TinyShield
from usaspending_api.references.abbreviations import code_to_state, fips_to_code, pad_codes
from usaspending_api.references.models import Cfda
from usaspending_api.search.v2.elasticsearch_helper import search_transactions2
from usaspending_api.search.v2.elasticsearch_helper import spending_by_transaction_count
from usaspending_api.search.v2.elasticsearch_helper import spending_by_transaction_sum_and_count


class SpendingByTransactionVisualizationViewSet(APIView):
    """
    This route takes keyword search fields, and returns the fields of the searched term.
        endpoint_doc: /advanced_award_search/spending_by_transaction.md
    """
    @total_ordering
    class MinType(object):
        def __le__(self, other):
            return True

        def __eq__(self, other):
            return self is other
    Min = MinType()

    @cache_response()
    def post(self, request):
        print('called post')

        # models = [
        #     {'name': 'fields', 'key': 'fields', 'type': 'array', 'array_type': 'text', 'text_type': 'search'},
        # ]
        # models.extend(AWARD_FILTER)
        # models.extend(PAGINATION)
        # for m in models:
        #     if m['name'] in ('keyword', 'award_type_codes', 'sort'):
        #         m['optional'] = False
        # validated_payload = TinyShield(models).block(request.data)

        # if validated_payload['sort'] not in validated_payload['fields']:
        #     raise InvalidParameterException("Sort value not found in fields: {}".format(validated_payload['sort']))
        
        # lower_limit = (validated_payload['page'] - 1) * validated_payload['limit']


        request_data = request.data
        # print(request_data)
        # success, response, total = search_transactions2(request_data, lower_limit, validated_payload['limit'] + 1)
        success, response, total = search_transactions2(request_data, 0, 60)

        if not success:
            raise InvalidParameterException(response)

        results = []
        for transaction in response:
            results.append(transaction)

        response = {
            'limit': 60,
            'results': results,
            'page_metadata': 'boom'
        }
        return Response(response)


class TransactionSummaryVisualizationViewSet(APIView):
    """
    This route takes award filters, and returns the number of transactions and summation of federal action obligations.
        endpoint_doc: /advanced_award_search/transaction_spending_summary.md
    """
    @cache_response()
    def post(self, request):
        """
            Returns a summary of transactions which match the award search filter
                Desired values:
                    total number of transactions `award_count`
                    The federal_action_obligation sum of all those transactions `award_spending`

            *Note* Only deals with prime awards, future plans to include sub-awards.
        """

        models = [{'name': 'keyword', 'key': 'filters|keyword', 'type': 'text', 'text_type': 'search', 'min': 3}]
        validated_payload = TinyShield(models).block(request.data)

        results = spending_by_transaction_sum_and_count(validated_payload)
        if not results:
            raise ElasticsearchConnectionException('Error generating the transaction sums and counts')
        return Response({"results": results})


class SpendingByTransactionCountVisualizaitonViewSet(APIView):
    """
    This route takes keyword search fields, and returns the fields of the searched term.
        endpoint_doc: /advanced_award_search/spending_by_transaction_count.md

    """
    @cache_response()
    def post(self, request):

        models = [{'name': 'keyword', 'key': 'filters|keyword', 'type': 'text', 'text_type': 'search', 'min': 3}]
        validated_payload = TinyShield(models).block(request.data)

        results = spending_by_transaction_count(validated_payload)
        if not results:
            raise ElasticsearchConnectionException('Error during the aggregations')
        return Response({"results": results})
