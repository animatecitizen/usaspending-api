# -*- coding: utf-8 -*-
# Generated by Django 1.11.4 on 2018-01-31 17:11
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('references', '0010_auto_20180130_1647'),
    ]

    operations = [
        migrations.AlterField(
            model_name='legalentity',
            name='airport_authority',
            field=models.BooleanField(default=False, verbose_name='Airport Authority'),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='alaskan_native_owned_corporation_or_firm',
            field=models.BooleanField(default=False, verbose_name='Alaskan Native Owned Corporation or Firm'),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='alaskan_native_servicing_institution',
            field=models.BooleanField(default=False, verbose_name='Alaskan Native Owned Servicing Institution'),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='american_indian_owned_business',
            field=models.BooleanField(default=False, verbose_name='American Indian Owned Business'),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='asian_pacific_american_owned_business',
            field=models.BooleanField(default=False, verbose_name='Asian Pacific American Owned business'),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='black_american_owned_business',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='c1862_land_grant_college',
            field=models.BooleanField(db_column='1862_land_grant_college', default=False, max_length=1, verbose_name='1862 Land Grant College'),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='c1890_land_grant_college',
            field=models.BooleanField(db_column='1890_land_grant_college', default=False, max_length=1, verbose_name='1890 Land Grant College'),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='c1994_land_grant_college',
            field=models.BooleanField(db_column='1994_land_grant_college', default=False, max_length=1, verbose_name='1894 Land Grant College'),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='c8a_program_participant',
            field=models.BooleanField(db_column='8a_program_participant', default=False, max_length=1, verbose_name='8a Program Participant'),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='city_local_government',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='community_developed_corporation_owned_firm',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='community_development_corporation',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='contracts',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='corporate_entity_not_tax_exempt',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='corporate_entity_tax_exempt',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='council_of_governments',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='county_local_government',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='domestic_shelter',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='dot_certified_disadvantage',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='economically_disadvantaged_women_owned_small_business',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='educational_institution',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='emerging_small_business',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='federal_agency',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='federally_funded_research_and_development_corp',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='for_profit_organization',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='foreign_government',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='foreign_owned_and_located',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='foundation',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='grants',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='hispanic_american_owned_business',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='hispanic_servicing_institution',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='historically_black_college',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='historically_underutilized_business_zone',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='hospital_flag',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='housing_authorities_public_tribal',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='indian_tribe_federally_recognized',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='inter_municipal_local_government',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='international_organization',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='interstate_entity',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='joint_venture_economic_disadvantaged_women_owned_small_bus',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='joint_venture_women_owned_small_business',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='labor_surplus_area_firm',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='limited_liability_corporation',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='local_government_owned',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='manufacturer_of_goods',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='minority_institution',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='minority_owned_business',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='municipality_local_government',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='native_american_owned_business',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='native_hawaiian_owned_business',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='native_hawaiian_servicing_institution',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='nonprofit_organization',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='other_minority_owned_business',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='other_not_for_profit_organization',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='partnership_or_limited_liability_partnership',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='planning_commission',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='port_authority',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='private_university_or_college',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='receives_contracts_and_grants',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='sba_certified_8a_joint_venture',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='school_district_local_government',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='school_of_forestry',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='self_certified_small_disadvantaged_business',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='service_disabled_veteran_owned_business',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='small_agricultural_cooperative',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='small_disadvantaged_business',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='sole_proprietorship',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='state_controlled_institution_of_higher_learning',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='subchapter_scorporation',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='subcontinent_asian_asian_indian_american_owned_business',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='the_ability_one_program',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='township_local_government',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='transit_authority',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='tribal_college',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='tribally_owned_business',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='us_federal_government',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='us_government_entity',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='us_local_government',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='us_state_government',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='us_tribal_government',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='veteran_owned_business',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='veterinary_college',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='veterinary_hospital',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='woman_owned_business',
            field=models.BooleanField(default=False),
        ),
        migrations.AlterField(
            model_name='legalentity',
            name='women_owned_small_business',
            field=models.BooleanField(default=False),
        ),
    ]
