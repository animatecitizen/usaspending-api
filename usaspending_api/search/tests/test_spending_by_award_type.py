import json
import pytest

from rest_framework import status
from usaspending_api.search.tests.test_mock_data_search import all_filters


@pytest.mark.django_db
def test_spending_by_award_type_success(client, refresh_matviews):

    # test small request
    resp = client.post(
        '/api/v2/search/spending_by_award/',
        content_type='application/json',
        data=json.dumps({
            "fields": ["Award ID", "Recipient Name"],
            "filters": {
                "award_type_codes": ["A", "B", "C"]
            }
        }))
    assert resp.status_code == status.HTTP_200_OK

    # test all features
    resp = client.post(
        '/api/v2/search/spending_by_award',
        content_type='application/json',
        data=json.dumps({
            "fields": ["Award ID", "Recipient Name"],
            "filters": all_filters()
        }))
    assert resp.status_code == status.HTTP_200_OK

    # test subawards
    resp = client.post(
        '/api/v2/search/spending_by_award',
        content_type='application/json',
        data=json.dumps({
            "fields": ["Sub-Award ID"],
            "filters": all_filters(),
            "subawards": True
        }))
    assert resp.status_code == status.HTTP_200_OK


@pytest.mark.django_db
def test_spending_by_award_type_failure(client, refresh_matviews):
    """Verify error on bad autocomplete request for budget function."""

    resp = client.post(
        '/api/v2/search/spending_by_award/',
        content_type='application/json',
        data=json.dumps({'filters': {}}))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
