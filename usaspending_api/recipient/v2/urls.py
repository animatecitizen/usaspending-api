from django.conf.urls import url
from usaspending_api.recipient.v2.views import recipient

urlpatterns = [
    url(r'^duns/(?P<duns>[\w\d]+)$', recipient.DUNSViewSet.as_view())
    # Commented out until LEI is implemented
    # url(r'^lei/(?P<lei>[0-9]+)$', recipient.LEIViewSet.as_view())
]
