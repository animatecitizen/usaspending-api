from django.db import models

class DUNS(models.Model):
    """ DUNS Records """
    awardee_or_recipient_uniqu = models.TextField(db_index=True)
    legal_business_name = models.TextField(blank=True, null=True)
    activation_date = models.DateField(db_index=True, blank=True, null=True)
    deactivation_date = models.DateField(db_index=True, blank=True, null=True)
    registration_date = models.DateField(db_index=True, blank=True, null=True)
    expiration_date = models.DateField(db_index=True, blank=True, null=True)
    last_sam_mod_date = models.DateField(blank=True, null=True)
    ultimate_parent_unique_ide = models.TextField(db_index=True, blank=True, null=True)
    ultimate_parent_legal_enti = models.TextField(blank=True, null=True)

    created_at = models.DateTimeField(blank=True, null=True)
    updated_at = models.DateTimeField(blank=True, null=True)

    class Meta:
        db_table = 'duns'