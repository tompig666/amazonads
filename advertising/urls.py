# from django.contrib import admin
from django.conf.urls import url
from . import views


urlpatterns = [
    url('authUrl/', views.AuthUrl.as_view()),
    url('auth/', views.Auth.as_view()),
    url('sellerProfiles/', views.Profile.as_view()),
    url('campaigns/', views.Campaigns.as_view()),
    url('adGroups/', views.AdGroups.as_view()),
    # campaign negative keywords urls.
    url(r'^campaignNegativeKeywords/$',
        views.CampaignNegativeKeywords.as_view()),
    url(r'campaignNegativeKeywordsSearch/$',
        views.CampaignNegativeKeywordsSearch.as_view()),
    # adgroup negative keywords urls.
    url(r'^negativeKeywords/$',
        views.AdGroupNegativeKeywords.as_view()),
    url(r'negativeKeywordsSearch/$',
        views.AdGroupNegativeKeywordsSearch.as_view())
]
