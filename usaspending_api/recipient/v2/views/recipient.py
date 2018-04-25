import logging

from usaspending_api.references.abbreviations import pad_codes
from usaspending_api.awards.models import Award, Subaward
from usaspending_api.references.models import LegalEntity
from usaspending_api.common.models import FiscalYearFunc
from usaspending_api.common.views import APIDocumentationView

from django.db.models import Q, Sum
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response

logger = logging.getLogger(__name__)


class LegalEntityViewSet(APIDocumentationView):
    """
    Return an legal entity data
    endpoint_doc: /legal_entity.md
    """
    @cache_response()
    def get(self, request, id, type='recipient_unique_id'):
        """
        Return the view's queryset.
        """
        response = {'results': {}}

        logger.info('Finding legal entities with {} as {}'.format(type, id))
        le = LegalEntity.objects.filter(Q(**{type: id}))
        if not le.count():
            return Response(response)
        elif le.count() > 1:
            logger.info('Found multiple legal entities with {} as {}. Selecting the first one.'.format(type, id))
        le = le.first()
        parent_le = LegalEntity.objects.filter(recipient_unique_id=le.parent_recipient_unique_id).first()
        location = {} if not le.location else {
            'address_line1': le.location.address_line1,
            'address_line2': le.location.address_line2,
            'address_line3': le.location.address_line3,
            'city_name': le.location.city_name,
            'state_code': le.location.state_code,
            'zip5': le.location.zip5,
            'location_country_code': le.location.location_country_code,
            'country_name': le.location.country_name,
            'congressional_code': pad_codes('congressional_code', le.location.congressional_code)
        }
        # Awards Amounts
        awards_qs = Award.objects.filter(**{'recipient__{}'.format(type): id}) \
            .values('latest_transaction__action_date', 'total_obligation') \
            .annotate(fy=FiscalYearFunc('latest_transaction__action_date'))\
            .values('fy', 'total_obligation').order_by('-fy')
        awards_count = awards_qs.count()
        awards_total = (awards_qs.aggregate(award_total=Sum('total_obligation'))['award_total']) \
            if awards_count else 0
        # Subawards Amounts
        subawards_qs = Subaward.objects.filter(**{'recipient__{}'.format(type): id}).values('amount')
        subawards_count = subawards_qs.count()
        subawards_total = (subawards_qs.aggregate(subaward_total=Sum('amount'))['subaward_total']) \
            if subawards_count else 0

        fy = awards_qs.first()['fy']
        total = awards_total + subawards_total
        average = round(total / (awards_count + subawards_count), 2)
        amounts = {
            'fy':fy,
            'total':total,
            'average':average
        }
        response['results'] = {'name': le.recipient_name,
                               'duns': str(le.recipient_unique_id),
                               'parent_name': parent_le.recipient_name if parent_le else '',
                               'parent_duns': parent_le.recipient_unique_id if parent_le else '',
                               'location': location,
                               'business_categories': le.business_categories,
                               'amounts': amounts
                               }
        return Response(response)


class DUNSViewSet(LegalEntityViewSet):
    """
    Return an legal entity data based on DUNS
    endpoint_doc: /legal_entity.md
    """
    @cache_response()
    def get(self, request, duns, format=None):
        """
        Return the view's queryset.
        """
        return LegalEntityViewSet.get(self, request, duns, type='recipient_unique_id')


class LEIViewSet(LegalEntityViewSet):
    """
    Return an legal entity data based on LEI
    endpoint_doc: /legal_entity.md
    """
    @cache_response()
    def get(self, request, lei, format=None):
        """
        Return the view's queryset.
        """
        return LegalEntityViewSet.get(self, request, lei, type='lei')