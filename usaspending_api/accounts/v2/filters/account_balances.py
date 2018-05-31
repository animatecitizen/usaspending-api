import logging

from django.db.models import Case, CharField, OuterRef, Subquery, Value, When
from django.db.models.functions import Concat

from usaspending_api.accounts.helpers import start_and_end_dates_from_fyq
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.references.models import ToptierAgency

logger = logging.getLogger(__name__)
DERIVED_FIELDS = ['treasury_account_symbol', 'allocation_transfer_agency_name', 'agency_name']


def account_balances_filter(filters):
    query_filters = {}

    # Filter by agency if necessary
    if filters.get('agency', False) and filters['agency'] != 'all':
        agency = ToptierAgency.objects.filter(toptier_agency_id=filters['agency']).first()
        if agency:
            query_filters['treasury_account_identifier__agency_id'] = agency.cgac_code

    # TODO: Filter by federal account
    # federal_account = filters.get('federal_account', False)
    # if federal_account:
    #     query_filters['treasury_account_identifier__federal_account__federal_account_code'] = federal_account

    # Filter by Fiscal Year and Quarter
    if filters.get('fy', False) and filters.get('quarter', False):
        start_date, end_date = start_and_end_dates_from_fyq(filters['fy'], filters['quarter'])
        query_filters['reporting_period_start'] = start_date
        query_filters['reporting_period_end'] = end_date
    else:
        raise InvalidParameterException('fy and quarter are required parameters')

    # Derive treasury_account_symbol, allocation_transfer_agency_name, and agency_name
    ata_subquery = ToptierAgency.objects.filter(
        cgac_code=OuterRef('treasury_account_identifier__allocation_transfer_agency_id'))
    agency_name_subquery = ToptierAgency.objects.filter(cgac_code=OuterRef('treasury_account_identifier__agency_id'))
    queryset = AppropriationAccountBalances.objects.annotate(
        treasury_account_symbol=Concat(
            'treasury_account_identifier__agency_id',
            Value('-'),
            Case(When(treasury_account_identifier__availability_type_code='X', then=Value('X')),
                 default=Concat('treasury_account_identifier__beginning_period_of_availability', Value('/'),
                                'treasury_account_identifier__ending_period_of_availability'),
                 output_field=CharField()),
            Value('-'),
            'treasury_account_identifier__main_account_code',
            Value('-'),
            'treasury_account_identifier__sub_account_code',
            output_field=CharField()),
        allocation_transfer_agency_name=Subquery(ata_subquery.values('name')[:1]),
        agency_name=Subquery(agency_name_subquery.values('name')[:1])
    )

    return queryset.filter(**query_filters)
