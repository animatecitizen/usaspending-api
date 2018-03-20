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
from usaspending_api.search.v2.elasticsearch_helper import search_transactions
from usaspending_api.search.v2.elasticsearch_helper import spending_by_transaction_count
from usaspending_api.search.v2.elasticsearch_helper import spending_by_transaction_sum_and_count


from usaspending_api.search.v2.elasticsearch_helper import search_transactions2, spending_over_time
from usaspending_api.search.v2.elasticsearch_helper import spending_by_geography


logger = logging.getLogger(__name__)


class SpendingOverTimeVisualizationViewSet(APIView):
    """
    This route takes award filters, and returns spending by time. The amount of time is denoted by the "group" value.
    endpoint_doc: /advanced_award_search/spending_over_time.md
    """
    @cache_response()
    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""
        json_request = request.data
        group = json_request.get('group', None)
        filters = json_request.get('filters', None)

        if group is None:
            raise InvalidParameterException('Missing one or more required request parameters: group')
        if filters is None:
            raise InvalidParameterException('Missing one or more required request parameters: filters')
        potential_groups = ['quarter', 'fiscal_year', 'month', 'fy', 'q', 'm']
        if group not in potential_groups:
            raise InvalidParameterException('group does not have a valid value')

        results = spending_over_time(request.data)
        print('got results')
        response = dict(results=results)
        return Response(response)


class SpendingByCategoryVisualizationViewSet(APIView):
    """
    This route takes award filters, and returns spending by the defined category/scope.
    The category is defined by the category keyword, and the scope is defined by is denoted by the scope keyword.
    endpoint_doc: /advanced_award_search/spending_by_category.md
    """
    @cache_response()
    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""
        # TODO: check logic in name_dict[x]["aggregated_amount"] statements

        json_request = request.data
        category = json_request.get("category", None)
        scope = json_request.get("scope", None)
        filters = json_request.get("filters", None)
        limit = json_request.get("limit", 10)
        page = json_request.get("page", 1)

        lower_limit = (page - 1) * limit
        upper_limit = page * limit

        if category is None:
            raise InvalidParameterException("Missing one or more required request parameters: category")
        potential_categories = ["awarding_agency", "funding_agency", "recipient", "cfda_programs", "industry_codes"]
        if category not in potential_categories:
            raise InvalidParameterException("Category does not have a valid value")
        if (scope is None) and (category != "cfda_programs"):
            raise InvalidParameterException("Missing one or more required request parameters: scope")
        if filters is None:
            raise InvalidParameterException("Missing one or more required request parameters: filters")


        response = {"category": category, "scope": scope, "limit": limit, "results": results,
                    "page_metadata": page_metadata}
        return Response(response)


class SpendingByGeographyVisualizationViewSet(APIView):
    """
        This route takes award filters, and returns spending by state code, county code, or congressional district code.
        endpoint_doc: /advanced award search/spending_by_geography.md
    """
    geo_layer = None  # State, county or District
    geo_layer_filters = None  # Specific geo_layers to filter on
    queryset = None  # Transaction queryset
    geo_queryset = None  # Aggregate queryset based on scope

    @cache_response()
    def post(self, request):
        print('called spending by heography')
        json_request = request.data
        self.scope = json_request.get("scope")
        self.filters = json_request.get("filters", {})
        self.geo_layer = json_request.get("geo_layer")
        self.geo_layer_filters = json_request.get("geo_layer_filters")

        fields_list = []  # fields to include in the aggregate query

        loc_dict = {
            'state': 'state_code',
            'county': 'county_code',
            'district': 'congressional_code'
        }

        model_dict = {
            'place_of_performance': 'pop',
            'recipient_location': 'recipient_location'
        }

        # Build the query based on the scope fields and geo_layers
        # Fields not in the reference objects above then request is invalid

        scope_field_name = model_dict.get(self.scope)
        loc_field_name = loc_dict.get(self.geo_layer)
        loc_lookup = '{}_{}'.format(scope_field_name, loc_field_name)

        if scope_field_name is None:
            raise InvalidParameterException("Invalid request parameters: scope")

        if loc_field_name is None:
            raise InvalidParameterException("Invalid request parameters: geo_layer")

        self.queryset, self.matview_model = spending_by_geography(self.filters)

        if self.geo_layer == 'state':
            # State will have one field (state_code) containing letter A-Z
            kwargs = {
                '{}_country_code'.format(scope_field_name): 'USA',
                'federal_action_obligation__isnull': False
            }

            # Only state scope will add its own state code
            # State codes are consistent in db ie AL, AK
            fields_list.append(loc_lookup)

            state_response = {
                'scope': self.scope,
                'geo_layer': self.geo_layer,
                'results': self.state_results(kwargs, fields_list, loc_lookup)
            }

            return Response(state_response)

        else:
            # County and district scope will need to select multiple fields
            # State code is needed for county/district aggregation
            state_lookup = '{}_{}'.format(scope_field_name, loc_dict['state'])
            fields_list.append(state_lookup)

            # Adding regex to county/district codes to remove entries with letters since
            # can't be surfaced by map
            kwargs = {'federal_action_obligation__isnull': False}

            if self.geo_layer == 'county':
                # County name added to aggregation since consistent in db
                county_name = '{}_{}'.format(scope_field_name, 'county_name')
                fields_list.append(county_name)
                self.county_district_queryset(
                    kwargs,
                    fields_list,
                    loc_lookup,
                    state_lookup,
                    scope_field_name
                )

                county_response = {
                    'scope': self.scope,
                    'geo_layer': self.geo_layer,
                    'results': self.county_results(state_lookup, county_name)
                }

                return Response(county_response)
            else:
                self.county_district_queryset(
                    kwargs,
                    fields_list,
                    loc_lookup,
                    state_lookup,
                    scope_field_name
                )

                district_response = {
                    'scope': self.scope,
                    'geo_layer': self.geo_layer,
                    'results': self.district_results(state_lookup)
                }

                return Response(district_response)

    def state_results(self, filter_args, lookup_fields, loc_lookup):
        # Adding additional state filters if specified
        if self.geo_layer_filters:
            self.queryset = self.queryset.filter(**{'{}__{}'.format(loc_lookup, 'in'): self.geo_layer_filters})
        else:
            # Adding null filter for state for specific partial index
            # when not using geocode_filter
            filter_args['{}__isnull'.format(loc_lookup)] = False

        self.geo_queryset = self.queryset.filter(**filter_args) \
            .values(*lookup_fields)
        filter_types = self.filters['award_type_codes'] if 'award_type_codes' in self.filters else award_type_mapping
        self.geo_queryset = sum_transaction_amount(self.geo_queryset, filter_types=filter_types)

        # State names are inconsistent in database (upper, lower, null)
        # Used lookup instead to be consistent
        results = [
            {
                'shape_code': x[loc_lookup],
                'aggregated_amount': x['transaction_amount'],
                'display_name': code_to_state.get(x[loc_lookup], {'name': 'None'}).get('name').title()
            } for x in self.geo_queryset
        ]

        return results

    def county_district_queryset(self, kwargs, fields_list, loc_lookup, state_lookup, scope_field_name):
        # Filtering queryset to specific county/districts if requested
        # Since geo_layer_filters comes as concat of state fips and county/district codes
        # need to split for the geocode_filter
        if self.geo_layer_filters:
            self.queryset &= geocode_filter_locations(scope_field_name, [
                {'state': fips_to_code.get(x[:2]), self.geo_layer: x[2:], 'country': 'USA'}
                for x in self.geo_layer_filters
            ], self.matview_model, True)
        else:
            # Adding null,USA, not number filters for specific partial index
            # when not using geocode_filter
            kwargs['{}__{}'.format(loc_lookup, 'isnull')] = False
            kwargs['{}__{}'.format(state_lookup, 'isnull')] = False
            kwargs['{}_country_code'.format(scope_field_name)] = 'USA'
            kwargs['{}__{}'.format(loc_lookup, 'iregex')] = r'^[0-9]*(\.\d+)?$'

        # Turn county/district codes into float since inconsistent in database
        # Codes in location table ex: '01', '1', '1.0'
        # Cast will group codes as a float and will combine inconsistent codes
        self.geo_queryset = self.queryset.filter(**kwargs) \
            .values(*fields_list)
        self.geo_queryset = self.geo_queryset.annotate(code_as_float=Cast(loc_lookup, FloatField()))
        filter_types = self.filters['award_type_codes'] if 'award_type_codes' in self.filters else award_type_mapping
        self.geo_queryset = sum_transaction_amount(self.geo_queryset, filter_types=filter_types)

        return self.geo_queryset

    def county_results(self, state_lookup, county_name):
        # Returns county results formatted for map
        results = [
            {
                'shape_code': code_to_state.get(x[state_lookup])['fips'] +
                pad_codes(self.geo_layer, x['code_as_float']),
                'aggregated_amount': x['transaction_amount'],
                'display_name': x[county_name].title() if x[county_name] is not None
                else x[county_name]
            }
            for x in self.geo_queryset
        ]

        return results

    def district_results(self, state_lookup):
        # Returns congressional district results formatted for map
        results = [
            {
                'shape_code': code_to_state.get(x[state_lookup])['fips'] +
                pad_codes(self.geo_layer, x['code_as_float']),
                'aggregated_amount': x['transaction_amount'],
                'display_name': x[state_lookup] + '-' +
                pad_codes(self.geo_layer, x['code_as_float'])
            } for x in self.geo_queryset
        ]

        return results


class SpendingByAwardVisualizationViewSet(APIView):
    """
    This route takes award filters and fields, and returns the fields of the filtered awards.
    endpoint_doc: /advanced_award_search/spending_by_award.md
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
        """Return all budget function/subfunction titles matching the provided search text"""
        json_request = request.data
        fields = json_request.get("fields", None)
        filters = json_request.get("filters", None)
        order = json_request.get("order", "asc")
        limit = json_request.get("limit", 10)
        page = json_request.get("page", 1)

        lower_limit = (page - 1) * limit
        upper_limit = page * limit

        # results = []
        # for award in limited_queryset[:limit]:
        #     row = {"internal_id": award["award_id"]}

        #     if award['type'] in loan_type_mapping:  # loans
        #         for field in fields:
        #             row[field] = award.get(loan_award_mapping.get(field))
        #     elif award['type'] in non_loan_assistance_type_mapping:  # assistance data
        #         for field in fields:
        #             row[field] = award.get(non_loan_assistance_award_mapping.get(field))
        #     elif (award['type'] is None and award['piid']) or award['type'] in contract_type_mapping:  # IDV + contract
        #         for field in fields:
        #             row[field] = award.get(award_contracts_mapping.get(field))

        #     if "Award ID" in fields:
        #         for id_type in ["piid", "fain", "uri"]:
        #             if award[id_type]:
        #                 row["Award ID"] = award[id_type]
        #                 break
        #     results.append(row)
        # build response

        request_data = request.data
        success, response, total = search_transactions2(request_data, lower_limit, limit)

        if not success:
            raise InvalidParameterException(response)
        print('got response')
        results = []
        for transaction in response:
            results.append(transaction)

        response = {
            'limit': 60,
            'results': results,
            'page_metadata': 'boom'
        }
        return Response(response)



class SpendingByAwardCountVisualizationViewSet(APIView):
    """
    This route takes award filters, and returns the number of awards in each award type (Contracts, Loans, Grants, etc.)
        endpoint_doc: /advanced_award_search/spending_by_award_count.md
    """
    @cache_response()
    def post(self, request):
        """Return all budget function/subfunction titles matching the provided search text"""
        json_request = request.data
        filters = json_request.get("filters", None)
        if filters is None:
            raise InvalidParameterException("Missing one or more required request parameters: filters")

        queryset, model = spending_by_award_count(filters)
        if model == 'SummaryAwardView':
            queryset = queryset \
                .values("category") \
                .annotate(category_count=Sum('counts'))
        else:
            # for IDV CONTRACTS category is null. change to contract
            queryset = queryset \
                .values('category') \
                .annotate(category_count=Count(Coalesce('category', Value('contract')))) \
                .values('category', 'category_count')

        results = {"contracts": 0, "grants": 0, "direct_payments": 0, "loans": 0, "other": 0}

        categories = {
            'contract': 'contracts',
            'grant': 'grants',
            'direct payment': 'direct_payments',
            'loans': 'loans',
            'other': 'other'
        }

        # DB hit here
        for award in queryset:
            if award['category'] is None:
                result_key = 'contracts'
            elif award['category'] not in categories.keys():
                result_key = 'other'
            else:
                result_key = categories[award['category']]
            results[result_key] += award['category_count']

        # build response
        return Response({"results": results})


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

        models = [
            {'name': 'fields', 'key': 'fields', 'type': 'array', 'array_type': 'text', 'text_type': 'search'},
        ]
        models.extend(AWARD_FILTER)
        models.extend(PAGINATION)
        for m in models:
            if m['name'] in ('keyword', 'award_type_codes', 'sort'):
                m['optional'] = False
        validated_payload = TinyShield(models).block(request.data)

        if validated_payload['sort'] not in validated_payload['fields']:
            raise InvalidParameterException("Sort value not found in fields: {}".format(validated_payload['sort']))

        lower_limit = (validated_payload['page'] - 1) * validated_payload['limit']
        success, response, total = search_transactions(validated_payload, lower_limit, validated_payload['limit'] + 1)
        if not success:
            raise InvalidParameterException(response)

        metadata = get_simple_pagination_metadata(len(response), validated_payload['limit'], validated_payload['page'])

        results = []
        for transaction in response[:validated_payload['limit']]:
            results.append(transaction)

        response = {
            'limit': validated_payload['limit'],
            'results': results,
            'page_metadata': metadata
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
