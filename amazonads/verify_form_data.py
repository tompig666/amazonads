from django import forms


class SellerAdForms(forms.Form):
    profileId = forms.CharField(required=True, error_messages={'required': 'profileId can not be null',
                                                               'invalid': 'type of profileId must be CharField'})
    minDate = forms.DateField(required=True, error_messages={'required': 'minDate can not be null',
                                                             'invalid': 'type of minDate must be Date'})
    maxDate = forms.DateField(required=True, error_messages={'required': 'maxDate can not be null',
                                                             'invalid': 'type of maxDate must be Date'})


class CampaignForms(SellerAdForms, forms.Form):
    current = forms.IntegerField(required=True, error_messages={'required': 'current can not be null',
                                                                'invalid': 'type of current must be IntegerField'})
    size = forms.IntegerField(required=True, error_messages={'required': 'size can not be null',
                                                             'invalid': 'type of size must be IntegerField'})


class CampaignDetailForms(SellerAdForms, forms.Form):
    campaignId = forms.CharField(required=True, error_messages={'required': 'campaignId can not be null',
                                                                'invalid': 'type of campaignId must be CharField'})


class AdGroupForms(SellerAdForms, forms.Form):
    current = forms.IntegerField(required=True, error_messages={'required': 'current can not be null',
                                                                'invalid': 'type of current must be IntegerField'})
    size = forms.IntegerField(required=True, error_messages={'required': 'size can not be null',
                                                             'invalid': 'type of size must be IntegerField'})
    campaignId = forms.CharField(required=True, error_messages={'required': 'campaignId can not be null',
                                                                'invalid': 'type of campaignId must be CharField'})


class AdGroupDetailForms(SellerAdForms, forms.Form):
    adGroupId = forms.CharField(required=True, error_messages={'required': 'adGroupId can not be null',
                                                               'invalid': 'type of adGroupId must be CharField'})


class ProductAdForms(SellerAdForms, forms.Form):
    current = forms.IntegerField(required=True, error_messages={'required': 'current can not be null',
                                                                'invalid': 'type of current must be IntegerField'})
    size = forms.IntegerField(required=True, error_messages={'required': 'size can not be null',
                                                             'invalid': 'type of size must be IntegerField'})
    adGroupId = forms.CharField(required=True, error_messages={'required': 'adGroupId can not be null',
                                                               'invalid': 'type of adGroupId must be CharField'})


class ProductAdDetailForms(SellerAdForms, forms.Form):
    adId = forms.CharField(required=True, error_messages={'required': 'adId can not be null',
                                                          'invalid': 'type of adId must be CharField'})


class KeywordQueryForms(SellerAdForms, forms.Form):
    keywordId = forms.CharField(required=True, error_messages={'required': 'keywordId can not be null',
                                                               'invalid': 'type of keywordId must be CharField'})


class OperateAdForms(forms.Form):
    profileId = forms.CharField(required=True, error_messages={
        'required': 'profileId can not be null',
        'invalid': 'type of profileId must be CharField'})

    @classmethod
    def check_campaign(cls, data, is_update=True):
        entitys = data.get("campaign_entitys")
        for entity in entitys:
            if is_update:
                obj = CampaignPutForms(entity)
            else:
                obj = CampaignPostForms(entity)
            if not obj.is_valid():
                return obj
        return OperateAdForms(data)

    @classmethod
    def check_adgroup(cls, data, is_update=True):
        entitys = data.get("adgroup_entitys")
        for entity in entitys:
            if is_update:
                obj = AdGroupPutForms(entity)
            else:
                obj = AdGroupPostForms(entity)
            if not obj.is_valid():
                return obj
        return OperateAdForms(data)


class CampaignPutForms(forms.Form):
    campaignId = forms.CharField(required=True, error_messages={
        'required': 'campaignId can not be null',
        'invalid': 'type of campaignId must be CharField'})
    dailyBudget = forms.FloatField(error_messages={
        'invalid': 'type of dailyBudget must be FloatField'})


class CampaignPostForms(forms.Form):
    name = forms.CharField(required=True, error_messages={
        'required': 'name can not be null',
        'invalid': 'type of name must be CharField'})
    dailyBudget = forms.FloatField(required=True, error_messages={
        'required': 'dailyBudget can not be null',
        'invalid': 'type of dailyBudget must be FloatField'})
    targetingType = forms.ChoiceField(required=True, choices=(
        ('manual', 'manual'), ('auto', 'auto')), error_messages={
        'required': 'targetingType require one of manual,auto',
        'invalid': 'type of state must be CharField'})
    state = forms.ChoiceField(required=True, choices=(
        ('enabled', 'enabled'), ('paused', 'paused'), ('archived', 'archived')),
                              error_messages={
                                  'required': 'state require one of '
                                              'enabled,paused,archived',
                                  'invalid': 'type of state must be CharField'})
    startDate = forms.CharField(required=True, error_messages={
        'required': 'startDate can not be null',
        'invalid': 'type of startDate must be CharField'})


class AdGroupPutForms(forms.Form):
    adGroupId = forms.CharField(required=True, error_messages={
        'required': 'adGroupId can not be null',
        'invalid': 'type of adGroupId must be CharField'})
    defaultBid = forms.FloatField(error_messages={
        'invalid': 'type of defaultBid must be FloatField'})


class AdGroupPostForms(forms.Form):
    """
    Required fields for ad group creation are:
    campaignId, name, state, and defaultBid
    """
    campaignId = forms.CharField(required=True, error_messages={
        'required': 'campaignId can not be null',
        'invalid': 'type of campaignId must be CharField'})
    name = forms.CharField(required=True, error_messages={
        'required': 'name can not be null',
        'invalid': 'type of name must be CharField'})
    state = forms.ChoiceField(required=True, choices=(
        ('enabled', 'enabled'), ('paused', 'paused'), ('archived', 'archived')),
                              error_messages={
                                  'required': 'state require one of '
                                              'enabled,paused,archived',
                                  'invalid': 'type of state must be CharField'}
                              )
    defaultBid = forms.FloatField(required=True, error_messages={
        'required': 'defaultBid can not be null',
        'invalid': 'type of defaultBid must be FloatField'})
