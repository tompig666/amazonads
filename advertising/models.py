# This is an auto-generated Django model module.
# You'll have to do the following manually to clean this up:
#   * Rearrange models' order
#   * Make sure each model has one field with primary_key=True
#   * Make sure each ForeignKey has `on_delete` set to the desired behavior.
#   * Remove `managed = False` lines if you wish to allow Django to create, modify, and delete the table
# Feel free to rename the models, but don't rename db_table values or field names.
from django.db import models
from django_mysql.models import JSONField


class CustomerSeller(models.Model):
    customer_id = models.IntegerField()
    seller_uuid = models.CharField(max_length=36, unique=True)
    seller_email = models.CharField(max_length=128)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = 'customer_seller'


class SellerAuth(models.Model):
    seller = models.ForeignKey(CustomerSeller, to_field="seller_uuid",
                               db_column="seller_uuid", on_delete=models.CASCADE)
    access_token = models.CharField(max_length=1024)
    refresh_token = models.CharField(max_length=1024)
    expire_after = models.DateTimeField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    deleted_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'seller_auth'


class SellerProfile(models.Model):
    profile_id = models.CharField(max_length=36)
    seller = models.ForeignKey(CustomerSeller, to_field="seller_uuid",
                               db_column='seller_uuid', on_delete=models.CASCADE)
    country_code = models.CharField(max_length=36)
    currency_code = models.CharField(max_length=36)
    daily_budget = models.CharField(max_length=36)
    timezone = models.CharField(max_length=64)
    marketplace_string_id = models.CharField(max_length=64)
    amazon_account_id = models.CharField(max_length=64)
    amazon_account_type = models.CharField(max_length=64)
    status = models.CharField(max_length=16, default="")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = 'seller_profile'


class KeywordAggReport(models.Model):
    ad_group_id = models.CharField(max_length=36, null=False, default='')
    date_range = models.CharField(max_length=36, null=False, default='')
    data = JSONField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = 'keyword_agg_report'


class ProductAdAggReport(models.Model):
    ad_group_id = models.CharField(max_length=36, null=False, default='')
    date_range = models.CharField(max_length=36, null=False, default='')
    data = JSONField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = False
        db_table = 'product_ad_agg_report'
