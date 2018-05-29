import json
import pytest
import random

from django.conf import settings
from django.db import connection
from model_mommy import mommy
from rest_framework import status
from unittest.mock import Mock

from usaspending_api.awards.models import TransactionNormalized, TransactionFABS, TransactionFPDS
from usaspending_api.awards.v2.lookups.lookups import award_type_mapping
from usaspending_api.download.filestreaming import csv_generation
from usaspending_api.download.lookups import JOB_STATUS
from usaspending_api.etl.award_helpers import update_awards


@pytest.fixture
def award_data(db):
    # Populate job status lookup table
    for js in JOB_STATUS:
        mommy.make('download.JobStatus', job_status_id=js.id, name=js.name, description=js.desc)

    # Create Locations
    mommy.make('references.Location')

    # Create LE
    mommy.make('references.LegalEntity')

    # Create Awarding Top Agency
    ata1 = mommy.make(
        'references.ToptierAgency',
        name="Bureau of Things",
        cgac_code='100',
        website='http://test.com',
        mission='test',
        icon_filename='test')
    ata2 = mommy.make(
        'references.ToptierAgency',
        name="Bureau of Stuff",
        cgac_code='101',
        website='http://test.com',
        mission='test',
        icon_filename='test')

    # Create Awarding subs
    mommy.make('references.SubtierAgency', name="Bureau of Things")

    # Create Awarding Agencies
    aa1 = mommy.make('references.Agency', id=1, toptier_agency=ata1, toptier_flag=False)
    aa2 = mommy.make('references.Agency', id=2, toptier_agency=ata2, toptier_flag=False)

    # Create Funding Top Agency
    mommy.make(
        'references.ToptierAgency',
        name="Bureau of Money",
        cgac_code='102',
        website='http://test.com',
        mission='test',
        icon_filename='test')

    # Create Funding SUB
    mommy.make('references.SubtierAgency', name="Bureau of Things")

    # Create Funding Agency
    mommy.make('references.Agency', id=3, toptier_flag=False)

    # Create Awards
    award1 = mommy.make('awards.Award', category='contracts')
    award2 = mommy.make('awards.Award', category='contracts')
    award3 = mommy.make('awards.Award', category='assistance')

    # Create Transactions
    trann1 = mommy.make(
        TransactionNormalized,
        award=award1,
        action_date='2018-01-01',
        type=random.choice(list(award_type_mapping)),
        modification_number=1,
        awarding_agency=aa1)
    trann2 = mommy.make(
        TransactionNormalized,
        award=award2,
        action_date='2018-01-01',
        type=random.choice(list(award_type_mapping)),
        modification_number=1,
        awarding_agency=aa2)
    trann3 = mommy.make(
        TransactionNormalized,
        award=award3,
        action_date='2018-01-01',
        type=random.choice(list(award_type_mapping)),
        modification_number=1,
        awarding_agency=aa2)

    # Create TransactionContract
    mommy.make(TransactionFPDS, transaction=trann1, piid='tc1piid')
    mommy.make(TransactionFPDS, transaction=trann2, piid='tc2piid')

    # Create TransactionAssistance
    mommy.make(TransactionFABS, transaction=trann3, fain='ta1fain')

    # Set latest_award for each award
    update_awards()


@pytest.fixture
def account_data(db):
    # Populate job status lookup table
    for js in JOB_STATUS:
        mommy.make('download.JobStatus', job_status_id=js.id, name=js.name, description=js.desc)

    # Create TreasuryAppropriationAccount models
    tas1 = mommy.make('accounts.TreasuryAppropriationAccount', agency_id='-01')
    tas2 = mommy.make('accounts.TreasuryAppropriationAccount', agency_id='-01')
    tas3 = mommy.make('accounts.TreasuryAppropriationAccount', agency_id='-02')

    # Create Toptier Agencies matching the TreasuryAppropriationAccount models
    mommy.make('references.ToptierAgency', cgac_code='-01')
    mommy.make('references.ToptierAgency', cgac_code='-02')
    mommy.make('references.ToptierAgency', cgac_code=tas1.allocation_transfer_agency_id)
    mommy.make('references.ToptierAgency', cgac_code=tas2.allocation_transfer_agency_id)
    mommy.make('references.ToptierAgency', cgac_code=tas3.allocation_transfer_agency_id)

    # Create AppropriationAccountBalances models
    mommy.make('accounts.AppropriationAccountBalances', treasury_account_identifier=tas1)
    mommy.make('accounts.AppropriationAccountBalances', treasury_account_identifier=tas1)
    mommy.make('accounts.AppropriationAccountBalances', treasury_account_identifier=tas2)
    mommy.make('accounts.AppropriationAccountBalances', treasury_account_identifier=tas3)

    # Create FinancialAccountsByProgramActivityObjectClass models
    mommy.make('financial_activities.FinancialAccountsByProgramActivityObjectClass', treasury_account=tas1)
    mommy.make('financial_activities.FinancialAccountsByProgramActivityObjectClass', treasury_account=tas1)
    mommy.make('financial_activities.FinancialAccountsByProgramActivityObjectClass', treasury_account=tas2)
    mommy.make('financial_activities.FinancialAccountsByProgramActivityObjectClass', treasury_account=tas3)


@pytest.mark.django_db
@pytest.mark.skip
def test_download_transactions_v2_endpoint(client, award_data):
    """test the transaction endpoint."""

    resp = client.post(
        '/api/v2/download/transactions',
        content_type='application/json',
        data=json.dumps({
            "filters": {"award_type_codes": []},
            "columns": {}
        }))

    assert resp.status_code == status.HTTP_200_OK
    assert '.zip' in resp.json()['url']


@pytest.mark.django_db
@pytest.mark.skip
def test_download_awards_v2_endpoint(client, award_data):
    """test the awards endpoint."""

    resp = client.post(
        '/api/v2/download/awards',
        content_type='application/json',
        data=json.dumps({
            "filters": {"award_type_codes": []},
            "columns": []
        }))

    assert resp.status_code == status.HTTP_200_OK
    assert '.zip' in resp.json()['url']


@pytest.mark.django_db
def test_download_accounts_a_v2_endpoint(client, account_data):
    """test the accounts endpoint."""
    db = connection.cursor().db.settings_dict
    connection_string = 'postgres://{}:{}@{}:5432/{}'.format(db['USER'], db['PASSWORD'], db['HOST'], db['NAME'])
    csv_generation.retrieve_db_string = Mock(return_value=connection_string)

    resp = client.post(
        '/api/v2/download/accounts',
        content_type='application/json',
        data=json.dumps({
            "account_level": "treasury_account",
            "filters": {
                "submission_type": "account_balances",
                "fy": "2017",
                "quarter": "4"
            },
            "file_format": "csv"
        }))

    assert resp.status_code == status.HTTP_200_OK
    assert '.zip' in resp.json()['url']


@pytest.mark.django_db
def test_download_accounts_b_v2_endpoint(client, account_data):
    """test the accounts endpoint."""
    db = connection.cursor().db.settings_dict
    connection_string = 'postgres://{}:{}@{}:5432/{}'.format(db['USER'], db['PASSWORD'], db['HOST'], db['NAME'])
    csv_generation.retrieve_db_string = Mock(return_value=connection_string)

    resp = client.post(
        '/api/v2/download/accounts',
        content_type='application/json',
        data=json.dumps({
            "account_level": "treasury_account",
            "filters": {
                "submission_type": "object_class_program_activity",
                "fy": "2017",
                "quarter": "4"
            },
            "file_format": "csv"
        }))

    assert resp.status_code == status.HTTP_200_OK
    assert '.zip' in resp.json()['url']


@pytest.mark.django_db
def test_download_accounts_c_v2_endpoint(client, account_data):
    """test the accounts endpoint."""
    db = connection.cursor().db.settings_dict
    connection_string = 'postgres://{}:{}@{}:5432/{}'.format(db['USER'], db['PASSWORD'], db['HOST'], db['NAME'])
    csv_generation.retrieve_db_string = Mock(return_value=connection_string)

    resp = client.post(
        '/api/v2/download/accounts',
        content_type='application/json',
        data=json.dumps({
            "account_level": "treasury_account",
            "filters": {
                "submission_type": "award_financial",
                "fy": "2017",
                "quarter": "4"
            },
            "file_format": "csv"
        }))

    assert resp.status_code == status.HTTP_200_OK
    assert '.zip' in resp.json()['url']


@pytest.mark.django_db
@pytest.mark.skip
def test_download_transactions_v2_status_endpoint(client, award_data):
    """Test the transactions status endpoint."""

    dl_resp = client.post(
        '/api/v2/download/transactions',
        content_type='application/json',
        data=json.dumps({
            "filters": {"award_type_codes": []},
            "columns": []
        }))

    resp = client.get('/api/v2/download/status/?file_name={}'.format(dl_resp.json()['file_name']))

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()['total_rows'] == 3
    assert resp.json()['total_columns'] > 100


@pytest.mark.django_db
@pytest.mark.skip
def test_download_awards_v2_status_endpoint(client, award_data):
    """Test the awards status endpoint."""

    dl_resp = client.post(
        '/api/v2/download/awards',
        content_type='application/json',
        data=json.dumps({
            "filters": {"award_type_codes": []},
            "columns": []
        }))

    resp = client.get('/api/v2/download/status/?file_name={}'
                      .format(dl_resp.json()['file_name']))

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json(
    )['total_rows'] == 3  # 2 awards, but 1 file with 2 rows and 1 file with 1``0`
    assert resp.json()['total_columns'] > 100


@pytest.mark.django_db
@pytest.mark.skip
def test_download_transactions_v2_endpoint_column_limit(client, award_data):
    """Test the transaction status endpoint's col selection."""

    # columns from both transaction_contract and transaction_assistance
    dl_resp = client.post(
        '/api/v2/download/transactions',
        content_type='application/json',
        data=json.dumps({
            "filters": {"award_type_codes": []},
            "columns": ["award_id_piid", "modification_number"]
        }))
    resp = client.get('/api/v2/download/status/?file_name={}'.format(dl_resp.json()['file_name']))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()['total_rows'] == 3
    assert resp.json()['total_columns'] == 2


@pytest.mark.django_db
@pytest.mark.skip
def test_download_transactions_v2_endpoint_column_filtering(client, award_data):
    """Test the transaction status endpoint's filtering."""

    dl_resp = client.post(
        '/api/v2/download/transactions',
        content_type='application/json',
        data=json.dumps({
            "filters": {
                "agencies": [{
                    'type': 'awarding',
                    'tier': 'toptier',
                    'name': "Bureau of Things"
                }]
            },
            "columns": ["award_id_piid", "modification_number"]
        }))
    resp = client.get('/api/v2/download/status/?file_name={}'.format(dl_resp.json()['file_name']))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()['total_rows'] == 2

    dl_resp = client.post(
        '/api/v2/download/transactions',
        content_type='application/json',
        data=json.dumps({
            "filters": {
                "agencies": [{
                    'type': 'awarding',
                    'tier': 'toptier',
                    'name': "Bureau of Stuff"
                }, ]
            },
            "columns": ["award_id_piid", "modification_number"]
        }))
    resp = client.get('/api/v2/download/status/?file_name={}'.format(dl_resp.json()['file_name']))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()['total_rows'] == 1

    dl_resp = client.post(
        '/api/v2/download/transactions',
        content_type='application/json',
        data=json.dumps({
            "filters": {
                "agencies": [
                    {
                        'type': 'awarding',
                        'tier': 'toptier',
                        'name': "Bureau of Stuff"
                    },
                    {
                        'type': 'awarding',
                        'tier': 'toptier',
                        'name': "Bureau of Things"
                    }
                ]
            },
            "columns": ["award_id_piid", "modification_number"]
        }))
    resp = client.get('/api/v2/download/status/?file_name={}'.format(dl_resp.json()['file_name']))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()['total_rows'] == 3


@pytest.mark.skip
def test_download_transactions_v2_bad_column_list_raises(client):
    """Test that bad column list inputs raise appropriate responses."""

    # Nonexistent filter
    payload = {
        "filters": {"award_type_codes": []},
        "columns": ["modification_number", "bogus_column"]
    }
    resp = client.post(
        '/api/v2/download/transactions',
        content_type='application/json',
        data=json.dumps(payload))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert 'Unknown columns' in resp.json()['detail']
    assert 'bogus_column' in resp.json()['detail']
    assert 'modification_number' not in resp.json()['detail']


@pytest.mark.skip
def test_download_transactions_v2_bad_filter_raises(client):
    """Test that bad filter inputs raise appropriate responses."""

    # Nonexistent filter
    payload = {"filters": {"blort_codes": ['01', ], }, "columns": []}
    resp = client.post(
        '/api/v2/download/transactions',
        content_type='application/json',
        data=json.dumps(payload))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert 'Invalid filter' in resp.json()['detail']


@pytest.mark.skip
def test_download_transactions_v2_bad_filter_type_raises(client):
    """Test filters sent as wrong data type"""

    # Non-dictionary for filters
    payload = {"filters": '01', "columns": []}
    resp = client.post(
        '/api/v2/download/transactions',
        content_type='application/json',
        data=json.dumps(payload))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert 'Invalid filter' in resp.json()['detail']


@pytest.mark.skip
def test_download_transactions_v2_bad_filter_shape_raises(client):
    """Test filter with wrong internal shape"""

    payload = {
        "filters": {
            "agencies": [{
                'type': 'not a valid type',
                'tier': 'nor a valid tier',
                'name': "Bureau of Stuff"
            }]
        },
        "columns": []
    }
    resp = client.post(
        '/api/v2/download/transactions',
        content_type='application/json',
        data=json.dumps(payload))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert 'Invalid filter' in resp.json()['detail']


@pytest.mark.django_db
@pytest.mark.skip
def test_download_status_nonexistent_file_404(client):
    """Requesting status of nonexistent file should produce HTTP 404"""

    resp = client.get(
        '/api/v2/download/status/?file_name=there_is_no_such_file.zip')

    assert resp.status_code == status.HTTP_404_NOT_FOUND


@pytest.mark.django_db
@pytest.mark.skip
def test_download_transactions_limit(client, award_data):
    """Test limiting of csv results"""
    dl_resp = client.post(
        '/api/v2/download/transactions',
        content_type='application/json',
        data=json.dumps({
            "limit": 2,
            "filters": {"award_type_codes": []},
            "columns": []
        }))
    resp = client.get('/api/v2/download/status/?file_name={}'.format(dl_resp.json()['file_name']))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()['total_rows'] == 2


def test_download_transactions_bad_limit(client, award_data):
    """Test proper error when bad value passed for limit."""

    resp = client.post(
        '/api/v2/download/transactions',
        content_type='application/json',
        data=json.dumps({
            "limit": "wombats",
            "filters": {"award_type_codes": []},
            "columns": []
        }))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


def test_download_transactions_excessive_limit(client, award_data):
    """Test that user-specified limits beyond MAX_DOWNLOAD_LIMIT are rejected"""

    resp = client.post(
        '/api/v2/download/transactions',
        content_type='application/json',
        data=json.dumps({
            "limit": settings.MAX_DOWNLOAD_LIMIT + 1,
            "filters": {"award_type_codes": []},
            "columns": []
        }))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.skip
def test_download_transactions_count(client, award_data):
    """Test transaction count endpoint when filters return zero"""
    resp = client.post(
        '/api/v2/download/count',
        content_type='application/json',
        data=json.dumps({
            "filters": {
                "agencies": [
                    {
                        "type": "awarding",
                        "tier": "toptier",
                        "name": "Bureau of Things"
                    }
                ]
            }
        }))

    assert resp.json()['transaction_rows_gt_limit'] is False
