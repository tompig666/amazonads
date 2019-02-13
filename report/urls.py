"""amazonads URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.urls import path
from . import views


urlpatterns = [
    path('reportUpdateTime/', views.ReportUpdateTime.as_view()),
    path('initAccount/', views.InitAccount.as_view()),
    path('sellerCampaignSummaries/', views.SellerCampaignSummaries.as_view()),
    path('sellerCampaignTrend/', views.SellerCampaignTrend.as_view()),
    path('campaignList/', views.CampaignList.as_view()),
    path('adGroupSummaries/', views.AdGroupSummaries.as_view()),
    path('adGroupTrend/', views.AdGroupTrend.as_view()),
    path('adGroupList/', views.AdGroupList.as_view()),
    path('productAdSummaries/', views.ProductAdSummaries.as_view()),
    path('productAdTrend/', views.ProductAdTrend.as_view()),
    path('asinList/', views.AsinList.as_view()),
    path('keywordList/', views.KeywordList.as_view()),
    path('searchTermList/', views.SearchTermList.as_view()),
]
