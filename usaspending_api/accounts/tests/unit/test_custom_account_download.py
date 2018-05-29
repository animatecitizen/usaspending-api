import pytest

from django.core.exceptions import FieldError

from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.accounts.v2.filters.account_balances import DERIVED_FIELDS as a_derived_fields
from usaspending_api.accounts.v2.filters.award_financial import DERIVED_FIELDS as c_derived_fields
from usaspending_api.accounts.v2.filters.object_class_program_activity import DERIVED_FIELDS as b_derived_fields
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.download.v2.download_column_historical_lookups import query_paths
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass


@pytest.mark.django_db
def test_account_balances_tas_mapping():
    """ Ensure the account_balances column-level mappings retrieve data from valid DB columns. """
    try:
        a_file_values = query_paths['account_balances']['treasury_account'].values()

        AppropriationAccountBalances.objects.values(*[val for val in a_file_values if val not in a_derived_fields])
    except FieldError:
        assert False


@pytest.mark.django_db
def test_object_class_program_activity_tas_mapping():
    """ Ensure the object_class_program_activity column-level mappings retrieve data from valid DB columns. """
    try:
        b_file_values = query_paths['object_class_program_activity']['treasury_account'].values()

        FinancialAccountsByProgramActivityObjectClass.objects.values(*[val for val in b_file_values if val not in
                                                                       b_derived_fields])
    except FieldError:
        assert False


@pytest.mark.django_db
def test_award_financial_tas_mapping():
    """ Ensure the award_financial column-level mappings retrieve data from valid DB columns. """
    try:
        c_file_values = query_paths['award_financial']['treasury_account'].values()

        FinancialAccountsByAwards.objects.values(*[val for val in c_file_values if val not in c_derived_fields])
    except FieldError:
        assert False
