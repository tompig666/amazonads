from .models import CustomerSeller, SellerProfile
from report.report_retriever import SellerReportRetriver


class Operation:
    @staticmethod
    def update_customer_profile_status(client_id):
        """ update profile status to normal if campaign report exist."""
        sellers = CustomerSeller.objects.filter(customer_id=client_id)
        for seller in sellers:
            profiles = seller.sellerprofile_set.all()
            for profile in profiles:
                profile_id = profile.profile_id
                if SellerReportRetriver.hasCampaignPerfRecord(profile_id):
                        SellerProfile.objects.filter(
                            profile_id=profile_id,
                            status="analyse").update(status="normal")
