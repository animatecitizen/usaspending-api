CREATE INDEX IF NOT EXISTS tmp_group_distinct_fpds_idx ON detached_award_procurement USING BTREE(
    piid,
    parent_award_id,
    agency_id,
    referenced_idv_agency_iden,
    action_date DESC,
    award_modification_amendme DESC,
    transaction_number DESC);

CREATE INDEX IF NOT EXISTS  published_award_financial_assi_ordered_idx ON published_award_financial_assistance USING BTREE(
    fain,
    awarding_sub_tier_agency_c,
    action_date DESC,
    award_modification_amendme DESC);

CREATE INDEX IF NOT EXISTS published_award_financial_assi_ordered_2idx ON published_award_financial_assistance USING BTREE(
    uri,
    awarding_sub_tier_agency_c,
    action_date DESC,
    award_modification_amendme DESC);

--ANALYZE VERBOSE detached_award_procurement;

CREATE MATERIALIZED VIEW testing_awards_view AS
------------------------------------------------------------------------------
------------------------------------------------------------------------------
--                                                                          --
-- FPDS - detached_award_procurement                                        --
--                                                                          --
------------------------------------------------------------------------------
------------------------------------------------------------------------------
(SELECT DISTINCT ON (
        dap.piid,
        dap.parent_award_id,
        dap.agency_id,
        dap.referenced_idv_agency_iden
    )

    'CONT_AW_' ||
        COALESCE(dap.agency_id,'-NONE-') || '_' ||
        COALESCE(dap.referenced_idv_agency_iden,'-NONE-') || '_' ||
        COALESCE(dap.piid,'-NONE-') || '_' ||
        COALESCE(dap.parent_award_id,'-NONE-')  AS generated_unique_award_id,
    dap.contract_award_type                     AS type,
    dap.contract_award_type_desc                AS type_description,
    dap.agency_id                               AS agency_id,
    dap.referenced_idv_agency_iden              AS referenced_idv_agency_iden,
    dap.referenced_idv_agency_desc              AS referenced_idv_agency_desc,
    dap.multiple_or_single_award_i              AS multiple_or_single_award_i,
    dap.multiple_or_single_aw_desc              AS multiple_or_single_aw_desc,
    dap.type_of_idc                             AS type_of_idc,
    dap.type_of_idc_description                 AS type_of_idc_description,
    dap.piid                                    AS piid,
    dap.parent_award_id                         AS parent_award_piid,
    NULL::TEXT                                  AS fain,
    NULL::TEXT                                  AS uri,
    fpds_agg.total_obligation                   AS total_obligation,
    NULL::NUMERIC                               AS total_subsidy_cost,
    NULL::NUMERIC                               AS total_loan_value,
    NULL::NUMERIC                               AS total_funding_amount,
    dap.awarding_agency_code,
    dap.awarding_agency_name,
    dap.awarding_sub_tier_agency_c,
    dap.awarding_sub_tier_agency_n,
    dap.awarding_office_code,
    dap.awarding_office_name,
    dap.funding_agency_code,
    dap.funding_agency_name,
    dap.funding_sub_tier_agency_co              AS funding_sub_tier_agency_co,
    dap.funding_sub_tier_agency_na              AS funding_sub_tier_agency_na,
    dap.funding_office_code                     AS funding_office_code,
    dap.funding_office_name                     AS funding_office_name,
    fpds_agg.certified_date                     AS action_date,
    fpds_agg.date_signed,
    dap.award_description                       AS description,
    fpds_agg.period_of_performance_start_date,
    fpds_agg.period_of_performance_current_end_date,

    NULL::NUMERIC                               AS potential_total_value_of_award,
    fpds_agg.base_and_all_options_value         AS base_and_all_options_value,
    dap.last_modified::DATE                     AS last_modified_date,
    fpds_agg.certified_date,
    NULL::INTEGER                               AS record_type,
    dap.detached_award_proc_unique              AS latest_transaction_unique_id,
--    0 AS total_subaward_amount,
--    0 AS subaward_count,
    dap.pulled_from                             AS pulled_from,
    dap.product_or_service_code                 AS product_or_service_code,
    dap.product_or_service_co_desc              AS product_or_service_co_desc,
    dap.extent_competed                         AS extent_competed,
    dap.extent_compete_description              AS extent_compete_description,
    dap.type_of_contract_pricing                AS type_of_contract_pricing,
    dap.type_of_contract_pric_desc              AS type_of_contract_pric_desc,
    dap.contract_award_type_desc                AS contract_award_type_desc,
    dap.cost_or_pricing_data                    AS cost_or_pricing_data,
    dap.cost_or_pricing_data_desc               AS cost_or_pricing_data_desc,
    dap.domestic_or_foreign_entity              AS domestic_or_foreign_entity,
    dap.domestic_or_foreign_e_desc              AS domestic_or_foreign_e_desc,
    dap.fair_opportunity_limited_s              AS fair_opportunity_limited_s,
    dap.fair_opportunity_limi_desc              AS fair_opportunity_limi_desc,
    dap.foreign_funding                         AS foreign_funding,
    dap.foreign_funding_desc                    AS foreign_funding_desc,
    dap.interagency_contracting_au              AS interagency_contracting_au,
    dap.interagency_contract_desc               AS interagency_contract_desc,
    dap.major_program                           AS major_program,
    dap.price_evaluation_adjustmen              AS price_evaluation_adjustmen,
    dap.program_acronym                         AS program_acronym,
    dap.subcontracting_plan                     AS subcontracting_plan,
    dap.subcontracting_plan_desc                AS subcontracting_plan_desc,
    dap.multi_year_contract                     AS multi_year_contract,
    dap.multi_year_contract_desc                AS multi_year_contract_desc,
    dap.purchase_card_as_payment_m              AS purchase_card_as_payment_m,
    dap.purchase_card_as_paym_desc              AS purchase_card_as_paym_desc,
    dap.consolidated_contract                   AS consolidated_contract,
    dap.consolidated_contract_desc              AS consolidated_contract_desc,
    dap.solicitation_identifier                 AS solicitation_identifier,
    dap.solicitation_procedures                 AS solicitation_procedures,
    dap.solicitation_procedur_desc              AS solicitation_procedur_desc,
    dap.number_of_offers_received               AS number_of_offers_received,
    dap.other_than_full_and_open_c              AS other_than_full_and_open_c,
    dap.other_than_full_and_o_desc              AS other_than_full_and_o_desc,
    dap.commercial_item_acquisitio              AS commercial_item_acquisitio,
    dap.commercial_item_acqui_desc              AS commercial_item_acqui_desc,
    dap.commercial_item_test_progr              AS commercial_item_test_progr,
    dap.commercial_item_test_desc               AS commercial_item_test_desc,
    dap.evaluated_preference                    AS evaluated_preference,
    dap.evaluated_preference_desc               AS evaluated_preference_desc,
    dap.fed_biz_opps                            AS fed_biz_opps,
    dap.fed_biz_opps_description                AS fed_biz_opps_description,
    dap.small_business_competitive              AS small_business_competitive,
    dap.dod_claimant_program_code               AS dod_claimant_program_code,
    dap.dod_claimant_prog_cod_desc              AS dod_claimant_prog_cod_desc,
    dap.program_system_or_equipmen              AS program_system_or_equipmen,
    dap.program_system_or_equ_desc              AS program_system_or_equ_desc,
    dap.information_technology_com              AS information_technology_com,
    dap.information_technolog_desc              AS information_technolog_desc,
    dap.sea_transportation                      AS sea_transportation,
    dap.sea_transportation_desc                 AS sea_transportation_desc,
    dap.clinger_cohen_act_planning              AS clinger_cohen_act_planning,
    dap.clinger_cohen_act_pla_desc              AS clinger_cohen_act_pla_desc,
    dap.davis_bacon_act                         AS davis_bacon_act,
    dap.davis_bacon_act_descrip                 AS davis_bacon_act_descrip,
    dap.service_contract_act                    AS service_contract_act,
    dap.service_contract_act_desc               AS service_contract_act_desc,
    dap.walsh_healey_act                        AS walsh_healey_act,
    dap.walsh_healey_act_descrip                AS walsh_healey_act_descrip,
    dap.naics                                   AS naics,
    dap.naics_description                       AS naics_description,
    dap.idv_type                                AS idv_type,
    dap.idv_type_description                    AS idv_type_description,
    dap.type_set_aside                          AS type_set_aside,
    dap.type_set_aside_description              AS type_set_aside_description,
    NULL::TEXT                                  AS assistance_type,
    NULL::TEXT                                  AS business_funds_indicator,
    NULL::TEXT                                  AS business_types,
    NULL::TEXT                                  AS business_types_description,
    -- business_categories,
    NULL::TEXT                                  AS cfda_number,
    NULL::TEXT                                  AS cfda_title,
    NULL::TEXT                                  AS sai_number,

    -- recipient data
    dap.awardee_or_recipient_uniqu              AS recipient_unique_id, -- DUNS
    dap.awardee_or_recipient_legal              AS recipient_name,
    dap.ultimate_parent_unique_ide              AS parent_recipient_unique_id,

    -- business categories
    dap.legal_entity_address_line1              AS recipient_location_address_line1,
    dap.legal_entity_address_line2              AS recipient_location_address_line2,
    dap.legal_entity_address_line3              AS recipient_location_address_line3,

    -- foreign province
    NULL::TEXT                                  AS recipient_location_foreign_province,
    NULL::TEXT                                  AS recipient_location_foreign_city_name,
    NULL::TEXT                                  AS recipient_location_foreign_postal_code,

    -- country
    dap.legal_entity_country_code               AS recipient_location_country_code,
    dap.legal_entity_country_name               AS recipient_location_country_name,

    -- state
    dap.legal_entity_state_code                 AS recipient_location_state_code,
    dap.legal_entity_state_descrip              AS recipient_location_state_name,

    -- county
    dap.legal_entity_county_code                AS recipient_location_county_code,
    dap.legal_entity_county_name                AS recipient_location_county_name,

    -- city
    NULL::TEXT                                  AS recipient_location_city_code,
    dap.legal_entity_city_name                  AS recipient_location_city_name,

    -- zip
    dap.legal_entity_zip5                       AS recipient_location_zip5,

    -- congressional disctrict
    dap.legal_entity_congressional              AS recipient_location_congressional_code,

    -- ppop data
    NULL::TEXT                                  AS pop_code,

    -- foreign
    NULL::TEXT                                  AS pop_foreign_province,

    -- country
    dap.place_of_perform_country_c              AS pop_country_code,
    dap.place_of_perf_country_desc              AS pop_country_name,

    -- state
    dap.place_of_performance_state              AS pop_state_code,
    dap.place_of_perfor_state_desc              AS pop_state_name,

    -- county
    dap.place_of_perform_county_co              AS pop_county_code,
    dap.place_of_perform_county_na              AS pop_county_name,

    -- city
    dap.place_of_perform_city_name              AS pop_city_name,

    -- zip
    dap.place_of_performance_zip5               AS pop_zip5,

    -- congressional disctrict
    dap.place_of_performance_congr              AS pop_congressional_code
FROM
    detached_award_procurement AS dap,
LATERAL aggregate_fpds(
        dap.agency_id,
        dap.referenced_idv_agency_iden,
        dap.piid,
        dap.parent_award_id
    ) AS fpds_agg(
        agency_id                               TEXT,
        referenced_idv_agency_iden              TEXT,
        piid                                    TEXT,
        parent_award_id                         TEXT,
        total_obligation                        NUMERIC,
        base_and_all_options_value              NUMERIC,
        date_signed                             DATE,
        certified_date                          DATE,
        period_of_performance_start_date        DATE,
        period_of_performance_current_end_date  DATE
    )
ORDER BY
    dap.piid,
    dap.parent_award_id,
    dap.agency_id,
    dap.referenced_idv_agency_iden,
    dap.action_date DESC,
    dap.award_modification_amendme DESC,
    dap.transaction_number DESC)
------------------------------------------------------------------------------
------------------------------------------------------------------------------
--                                                                          --
-- FPDS - detached_award_procurement                                        --
--                                                                          --
------------------------------------------------------------------------------
------------------------------------------------------------------------------
UNION ALL
------------------------------------------------------------------------------
------------------------------------------------------------------------------
--                                                                          --
-- FABS type 2/3 - published_award_financial_assistance                     --
--                                                                          --
------------------------------------------------------------------------------
------------------------------------------------------------------------------
(SELECT
    DISTINCT ON (
        pafa.fain,
        pafa.awarding_sub_tier_agency_c)
   'ASST_AW_' ||
       COALESCE(pafa.awarding_sub_tier_agency_c,'-NONE-') || '_' ||
       COALESCE(pafa.fain, '-NONE-') || '_' ||
       '-NONE-' AS generated_unique_award_id,
   pafa.assistance_type AS type,
   CASE
       WHEN pafa.assistance_type = '02' THEN 'Block Grant'
       WHEN pafa.assistance_type = '03' THEN 'Formula Grant'
       WHEN pafa.assistance_type = '04' THEN 'Project Grant'
       WHEN pafa.assistance_type = '05' THEN 'Cooperative Agreement'
       WHEN pafa.assistance_type = '06' THEN 'Direct Payment for Specified Use'
       WHEN pafa.assistance_type = '07' THEN 'Direct Loan'
       WHEN pafa.assistance_type = '08' THEN 'Guaranteed/Insured Loan'
       WHEN pafa.assistance_type = '09' THEN 'Insurance'
       WHEN pafa.assistance_type = '10' THEN 'Direct Payment with Unrestricted Use'
       WHEN pafa.assistance_type = '11' THEN 'Other Financial Assistance'
   END AS type_description,
   NULL::TEXT AS agency_id,
   NULL::TEXT AS referenced_idv_agency_iden,
   NULL::TEXT AS referenced_idv_agency_desc,
   NULL::TEXT AS multiple_or_single_award_i,
   NULL::TEXT AS multiple_or_single_aw_desc,
   NULL::TEXT AS type_of_idc,
   NULL::TEXT AS type_of_idc_description,
   NULL::TEXT AS piid,
   NULL::TEXT AS parent_award_piid,
   pafa.fain AS fain,
   NULL::TEXT AS uri,
   fabs_agg.total_obligation,
   fabs_agg.total_subsidy_cost,
   fabs_agg.total_loan_value,
   fabs_agg.total_funding_amount,
   pafa.awarding_agency_code AS awarding_agency_code,
   pafa.awarding_agency_name AS awarding_agency_name,
   pafa.awarding_sub_tier_agency_c AS awarding_sub_tier_agency_c,
   pafa.awarding_sub_tier_agency_n AS awarding_sub_tier_agency_n,
   pafa.awarding_office_code AS awarding_office_code,
   pafa.awarding_office_name AS awarding_office_name,
   pafa.funding_agency_code AS funding_agency_code,
   pafa.funding_agency_name AS funding_agency_name,
   pafa.funding_sub_tier_agency_co AS funding_sub_tier_agency_co,
   pafa.funding_sub_tier_agency_na AS funding_sub_tier_agency_na,
   pafa.funding_office_code AS funding_office_code,
   pafa.funding_office_name AS funding_office_name,
   fabs_agg.certified_date as action_date,
   fabs_agg.date_signed,
   pafa.award_description AS description,
   fabs_agg.period_of_performance_start_date,
   fabs_agg.period_of_performance_current_end_date,
   NULL::NUMERIC AS potential_total_value_of_award,
   NULL::NUMERIC AS base_and_all_options_value,
   pafa.modified_at::DATE AS last_modified_date,
   fabs_agg.certified_date,
   pafa.record_type AS record_type,
   pafa.afa_generated_unique AS latest_transaction_unique_id,
--    0 AS total_subaward_amount,
--    0 AS subaward_count,
   NULL::TEXT AS pulled_from,
   NULL::TEXT AS product_or_service_code,
   NULL::TEXT AS product_or_service_co_desc,
   NULL::TEXT AS extent_competed,
   NULL::TEXT AS extent_compete_description,
   NULL::TEXT AS type_of_contract_pricing,
   NULL::TEXT AS type_of_contract_pric_desc,
   NULL::TEXT AS contract_award_type_desc,
   NULL::TEXT AS cost_or_pricing_data,
   NULL::TEXT AS cost_or_pricing_data_desc,
   NULL::TEXT AS domestic_or_foreign_entity,
   NULL::TEXT AS domestic_or_foreign_e_desc,
   NULL::TEXT AS fair_opportunity_limited_s,
   NULL::TEXT AS fair_opportunity_limi_desc,
   NULL::TEXT AS foreign_funding,
   NULL::TEXT AS foreign_funding_desc,
   NULL::TEXT AS interagency_contracting_au,
   NULL::TEXT AS interagency_contract_desc,
   NULL::TEXT AS major_program,
   NULL::TEXT AS price_evaluation_adjustmen,
   NULL::TEXT AS program_acronym,
   NULL::TEXT AS subcontracting_plan,
   NULL::TEXT AS subcontracting_plan_desc,
   NULL::TEXT AS multi_year_contract,
   NULL::TEXT AS multi_year_contract_desc,
   NULL::TEXT AS purchase_card_as_payment_m,
   NULL::TEXT AS purchase_card_as_paym_desc,
   NULL::TEXT AS consolidated_contract,
   NULL::TEXT AS consolidated_contract_desc,
   NULL::TEXT AS solicitation_identifier,
   NULL::TEXT AS solicitation_procedures,
   NULL::TEXT AS solicitation_procedur_desc,
   NULL::TEXT AS number_of_offers_received,
   NULL::TEXT AS other_than_full_and_open_c,
   NULL::TEXT AS other_than_full_and_o_desc,
   NULL::TEXT AS commercial_item_acquisitio,
   NULL::TEXT AS commercial_item_acqui_desc,
   NULL::TEXT AS commercial_item_test_progr,
   NULL::TEXT AS commercial_item_test_desc,
   NULL::TEXT AS evaluated_preference,
   NULL::TEXT AS evaluated_preference_desc,
   NULL::TEXT AS fed_biz_opps,
   NULL::TEXT AS fed_biz_opps_description,
   NULL::TEXT AS small_business_competitive,
   NULL::TEXT AS dod_claimant_program_code,
   NULL::TEXT AS dod_claimant_prog_cod_desc,
   NULL::TEXT AS program_system_or_equipmen,
   NULL::TEXT AS program_system_or_equ_desc,
   NULL::TEXT AS information_technology_com,
   NULL::TEXT AS information_technolog_desc,
   NULL::TEXT AS sea_transportation,
   NULL::TEXT AS sea_transportation_desc,
   NULL::TEXT AS clinger_cohen_act_planning,
   NULL::TEXT AS clinger_cohen_act_pla_desc,
   NULL::TEXT AS davis_bacon_act,
   NULL::TEXT AS davis_bacon_act_descrip,
   NULL::TEXT AS service_contract_act,
   NULL::TEXT AS service_contract_act_desc,
   NULL::TEXT AS walsh_healey_act,
   NULL::TEXT AS walsh_healey_act_descrip,
   NULL::TEXT AS naics,
   NULL::TEXT AS naics_description,
   NULL::TEXT AS idv_type,
   NULL::TEXT AS idv_type_description,
   NULL::TEXT AS type_set_aside,
   NULL::TEXT AS type_set_aside_description,
   pafa.assistance_type AS assistance_type,
   pafa.business_funds_indicator AS business_funds_indicator,
   pafa.business_types AS business_types,
   CASE
       WHEN UPPER(pafa.business_types) = 'A' THEN 'State government'
       WHEN UPPER(pafa.business_types) = 'B' THEN 'County Government'
       WHEN UPPER(pafa.business_types) = 'C' THEN 'City or Township Government'
       WHEN UPPER(pafa.business_types) = 'D' THEN 'Special District Government'
       WHEN UPPER(pafa.business_types) = 'E' THEN 'Regional Organization'
       WHEN UPPER(pafa.business_types) = 'F' THEN 'U.S. Territory or Possession'
       WHEN UPPER(pafa.business_types) = 'G' THEN 'Independent School District'
       WHEN UPPER(pafa.business_types) = 'H' THEN 'Public/State Controlled Institution of Higher Education'
       WHEN UPPER(pafa.business_types) = 'I' THEN 'Indian/Native American Tribal Government (Federally Recognized)'
       WHEN UPPER(pafa.business_types) = 'J' THEN 'Indian/Native American Tribal Government (Other than Federally Recognized)'
       WHEN UPPER(pafa.business_types) = 'K' THEN 'Indian/Native American Tribal Designated Organization'
       WHEN UPPER(pafa.business_types) = 'L' THEN 'Public/Indian Housing Authority'
       WHEN UPPER(pafa.business_types) = 'M' THEN 'Nonprofit with 501(c)(3) IRS Status (Other than Institution of Higher Education)'
       WHEN UPPER(pafa.business_types) = 'N' THEN 'Nonprofit without 501(c)(3) IRS Status (Other than Institution of Higher Education)'
       WHEN UPPER(pafa.business_types) = 'O' THEN 'Private Institution of Higher Education'
       WHEN UPPER(pafa.business_types) = 'P' THEN 'Individual'
       WHEN UPPER(pafa.business_types) = 'Q' THEN 'For-Profit Organization (Other than Small Business)'
       WHEN UPPER(pafa.business_types) = 'R' THEN 'Small Business'
       WHEN UPPER(pafa.business_types) = 'S' THEN 'Hispanic-serving Institution'
       WHEN UPPER(pafa.business_types) = 'T' THEN 'Historically Black Colleges and Universities (HBCUs)'
       WHEN UPPER(pafa.business_types) = 'U' THEN 'Tribally Controlled Colleges and Universities (TCCUs)'
       WHEN UPPER(pafa.business_types) = 'V' THEN 'Alaska Native and Native Hawaiian Serving Institutions'
       WHEN UPPER(pafa.business_types) = 'W' THEN 'Non-domestic (non-US) Entity'
       WHEN UPPER(pafa.business_types) = 'X' THEN 'Other'
       ELSE 'Unknown Types'
   END AS business_types_description,
--    compile_fabs_business_categories(pafa.business_types) AS business_categories,
   pafa.cfda_number AS cfda_number,
   pafa.cfda_title AS cfda_title,
   pafa.sai_number AS sai_number,

   -- recipient data
   pafa.awardee_or_recipient_uniqu AS recipient_unique_id,
   pafa.awardee_or_recipient_legal AS recipient_name,
   NULL::TEXT AS parent_recipient_unique_id,

   -- business categories
   pafa.legal_entity_address_line1 AS recipient_location_address_line1,
   pafa.legal_entity_address_line2 AS recipient_location_address_line2,
   pafa.legal_entity_address_line3 AS recipient_location_address_line3,

   -- foreign province
   pafa.legal_entity_foreign_provi AS recipient_location_foreign_province,
   pafa.legal_entity_foreign_city AS recipient_location_foreign_city_name,
   pafa.legal_entity_foreign_posta AS recipient_location_foreign_postal_code,

   -- country
   pafa.legal_entity_country_code AS recipient_location_country_code,
   pafa.legal_entity_country_name AS recipient_location_country_name,

   -- state
   pafa.legal_entity_state_code AS recipient_location_state_code,
   pafa.legal_entity_state_name AS recipient_location_state_name,

   -- county
   pafa.legal_entity_county_code AS recipient_location_county_code,
   pafa.legal_entity_county_name AS recipient_location_county_name,

   -- city
   pafa.legal_entity_city_code AS recipient_location_city_code,
   pafa.legal_entity_city_name AS recipient_location_city_name,

   -- zip
   pafa.legal_entity_zip5 AS recipient_location_zip5,

   -- congressional disctrict
   pafa.legal_entity_congressional AS recipient_location_congressional_code,

   -- ppop data
   pafa.place_of_performance_code AS pop_code,

   -- foreign
   pafa.place_of_performance_forei AS pop_foreign_province,

   -- country
   pafa.place_of_perform_country_c AS pop_country_code,
   pafa.place_of_perform_country_n AS pop_country_name,

   -- state
   pafa.place_of_perfor_state_code AS pop_state_code,
   pafa.place_of_perform_state_nam AS pop_state_name,

   -- county
   pafa.place_of_perform_county_co AS pop_county_code,
   pafa.place_of_perform_county_na AS pop_county_name,

   -- city
   pafa.place_of_performance_city AS pop_city_name,

   -- zip
   pafa.place_of_performance_zip5 AS pop_zip5,

   -- congressional disctrict
   pafa.place_of_performance_congr AS pop_congressional_code

FROM published_award_financial_assistance AS pafa,

LATERAL aggregate_fabs(
    pafa.awarding_sub_tier_agency_c,
    pafa.fain,
    pafa.uri,
    pafa.record_type
    ) AS fabs_agg(
        awarding_sub_tier_agency_c TEXT,
        fain TEXT,
        uri TEXT,
        total_obligation NUMERIC,
        total_subsidy_cost NUMERIC,
        total_loan_value NUMERIC,
        total_funding_amount NUMERIC,
        date_signed DATE,
        certified_date DATE,
        period_of_performance_start_date DATE,
        period_of_performance_current_end_date DATE)
WHERE
    pafa.record_type IN ('2', '3') AND
    is_active IS TRUE
ORDER BY
    pafa.fain,
    pafa.awarding_sub_tier_agency_c,
    pafa.action_date DESC,
    pafa.award_modification_amendme DESC)
------------------------------------------------------------------------------
------------------------------------------------------------------------------
--                                                                          --
-- FABS type 2/3 - published_award_financial_assistance                     --
--                                                                          --
------------------------------------------------------------------------------
------------------------------------------------------------------------------
UNION ALL
------------------------------------------------------------------------------
------------------------------------------------------------------------------
--                                                                          --
-- FABS type 1 - published_award_financial_assistance                       --
--                                                                          --
------------------------------------------------------------------------------
------------------------------------------------------------------------------
(SELECT
   DISTINCT ON (pafa.uri, pafa.awarding_sub_tier_agency_c)
   'ASST_AW_' ||
       COALESCE(pafa.awarding_sub_tier_agency_c,'-NONE-') || '_' ||
       '-NONE-' || '_' ||
       COALESCE(pafa.uri, '-NONE-') AS generated_unique_award_id,
   pafa.assistance_type AS type,
   CASE
       WHEN pafa.assistance_type = '02' THEN 'Block Grant'
       WHEN pafa.assistance_type = '03' THEN 'Formula Grant'
       WHEN pafa.assistance_type = '04' THEN 'Project Grant'
       WHEN pafa.assistance_type = '05' THEN 'Cooperative Agreement'
       WHEN pafa.assistance_type = '06' THEN 'Direct Payment for Specified Use'
       WHEN pafa.assistance_type = '07' THEN 'Direct Loan'
       WHEN pafa.assistance_type = '08' THEN 'Guaranteed/Insured Loan'
       WHEN pafa.assistance_type = '09' THEN 'Insurance'
       WHEN pafa.assistance_type = '10' THEN 'Direct Payment with Unrestricted Use'
       WHEN pafa.assistance_type = '11' THEN 'Other Financial Assistance'
   END AS type_description,
   NULL::TEXT AS agency_id,
   NULL::TEXT AS referenced_idv_agency_iden,
   NULL::TEXT AS referenced_idv_agency_desc,
   NULL::TEXT AS multiple_or_single_award_i,
   NULL::TEXT AS multiple_or_single_aw_desc,
   NULL::TEXT AS type_of_idc,
   NULL::TEXT AS type_of_idc_description,
   NULL::TEXT AS piid,
   NULL::TEXT AS parent_award_piid,
   NULL::TEXT AS fain,
   pafa.uri AS uri,
   fabs_agg.total_obligation,
   fabs_agg.total_subsidy_cost,
   fabs_agg.total_loan_value,
   fabs_agg.total_funding_amount,
   pafa.awarding_agency_code AS awarding_agency_code,
   pafa.awarding_agency_name AS awarding_agency_name,
   pafa.awarding_sub_tier_agency_c AS awarding_sub_tier_agency_c,
   pafa.awarding_sub_tier_agency_n AS awarding_sub_tier_agency_n,
   pafa.awarding_office_code AS awarding_office_code,
   pafa.awarding_office_name AS awarding_office_name,
   pafa.funding_agency_code AS funding_agency_code,
   pafa.funding_agency_name AS funding_agency_name,
   pafa.funding_sub_tier_agency_co AS funding_sub_tier_agency_co,
   pafa.funding_sub_tier_agency_na AS funding_sub_tier_agency_na,
   pafa.funding_office_code AS funding_office_code,
   pafa.funding_office_name AS funding_office_name,
   fabs_agg.certified_date as action_date,
   fabs_agg.date_signed,
   pafa.award_description AS description,
   fabs_agg.period_of_performance_start_date,
   fabs_agg.period_of_performance_current_end_date,
   NULL::NUMERIC AS potential_total_value_of_award,
   NULL::NUMERIC AS base_and_all_options_value,
   pafa.modified_at::DATE AS last_modified_date,
   fabs_agg.certified_date,
   pafa.record_type AS record_type,
   pafa.afa_generated_unique AS latest_transaction_unique_id,
--    0 AS total_subaward_amount,
--    0 AS subaward_count,
   NULL::TEXT AS pulled_from,
   NULL::TEXT AS product_or_service_code,
   NULL::TEXT AS product_or_service_co_desc,
   NULL::TEXT AS extent_competed,
   NULL::TEXT AS extent_compete_description,
   NULL::TEXT AS type_of_contract_pricing,
   NULL::TEXT AS type_of_contract_pric_desc,
   NULL::TEXT AS contract_award_type_desc,
   NULL::TEXT AS cost_or_pricing_data,
   NULL::TEXT AS cost_or_pricing_data_desc,
   NULL::TEXT AS domestic_or_foreign_entity,
   NULL::TEXT AS domestic_or_foreign_e_desc,
   NULL::TEXT AS fair_opportunity_limited_s,
   NULL::TEXT AS fair_opportunity_limi_desc,
   NULL::TEXT AS foreign_funding,
   NULL::TEXT AS foreign_funding_desc,
   NULL::TEXT AS interagency_contracting_au,
   NULL::TEXT AS interagency_contract_desc,
   NULL::TEXT AS major_program,
   NULL::TEXT AS price_evaluation_adjustmen,
   NULL::TEXT AS program_acronym,
   NULL::TEXT AS subcontracting_plan,
   NULL::TEXT AS subcontracting_plan_desc,
   NULL::TEXT AS multi_year_contract,
   NULL::TEXT AS multi_year_contract_desc,
   NULL::TEXT AS purchase_card_as_payment_m,
   NULL::TEXT AS purchase_card_as_paym_desc,
   NULL::TEXT AS consolidated_contract,
   NULL::TEXT AS consolidated_contract_desc,
   NULL::TEXT AS solicitation_identifier,
   NULL::TEXT AS solicitation_procedures,
   NULL::TEXT AS solicitation_procedur_desc,
   NULL::TEXT AS number_of_offers_received,
   NULL::TEXT AS other_than_full_and_open_c,
   NULL::TEXT AS other_than_full_and_o_desc,
   NULL::TEXT AS commercial_item_acquisitio,
   NULL::TEXT AS commercial_item_acqui_desc,
   NULL::TEXT AS commercial_item_test_progr,
   NULL::TEXT AS commercial_item_test_desc,
   NULL::TEXT AS evaluated_preference,
   NULL::TEXT AS evaluated_preference_desc,
   NULL::TEXT AS fed_biz_opps,
   NULL::TEXT AS fed_biz_opps_description,
   NULL::TEXT AS small_business_competitive,
   NULL::TEXT AS dod_claimant_program_code,
   NULL::TEXT AS dod_claimant_prog_cod_desc,
   NULL::TEXT AS program_system_or_equipmen,
   NULL::TEXT AS program_system_or_equ_desc,
   NULL::TEXT AS information_technology_com,
   NULL::TEXT AS information_technolog_desc,
   NULL::TEXT AS sea_transportation,
   NULL::TEXT AS sea_transportation_desc,
   NULL::TEXT AS clinger_cohen_act_planning,
   NULL::TEXT AS clinger_cohen_act_pla_desc,
   NULL::TEXT AS davis_bacon_act,
   NULL::TEXT AS davis_bacon_act_descrip,
   NULL::TEXT AS service_contract_act,
   NULL::TEXT AS service_contract_act_desc,
   NULL::TEXT AS walsh_healey_act,
   NULL::TEXT AS walsh_healey_act_descrip,
   NULL::TEXT AS naics,
   NULL::TEXT AS naics_description,
   NULL::TEXT AS idv_type,
   NULL::TEXT AS idv_type_description,
   NULL::TEXT AS type_set_aside,
   NULL::TEXT AS type_set_aside_description,
   pafa.assistance_type AS assistance_type,
   pafa.business_funds_indicator AS business_funds_indicator,
   pafa.business_types AS business_types,
   CASE
       WHEN UPPER(pafa.business_types) = 'A' THEN 'State government'
       WHEN UPPER(pafa.business_types) = 'B' THEN 'County Government'
       WHEN UPPER(pafa.business_types) = 'C' THEN 'City or Township Government'
       WHEN UPPER(pafa.business_types) = 'D' THEN 'Special District Government'
       WHEN UPPER(pafa.business_types) = 'E' THEN 'Regional Organization'
       WHEN UPPER(pafa.business_types) = 'F' THEN 'U.S. Territory or Possession'
       WHEN UPPER(pafa.business_types) = 'G' THEN 'Independent School District'
       WHEN UPPER(pafa.business_types) = 'H' THEN 'Public/State Controlled Institution of Higher Education'
       WHEN UPPER(pafa.business_types) = 'I' THEN 'Indian/Native American Tribal Government (Federally Recognized)'
       WHEN UPPER(pafa.business_types) = 'J' THEN 'Indian/Native American Tribal Government (Other than Federally Recognized)'
       WHEN UPPER(pafa.business_types) = 'K' THEN 'Indian/Native American Tribal Designated Organization'
       WHEN UPPER(pafa.business_types) = 'L' THEN 'Public/Indian Housing Authority'
       WHEN UPPER(pafa.business_types) = 'M' THEN 'Nonprofit with 501(c)(3) IRS Status (Other than Institution of Higher Education)'
       WHEN UPPER(pafa.business_types) = 'N' THEN 'Nonprofit without 501(c)(3) IRS Status (Other than Institution of Higher Education)'
       WHEN UPPER(pafa.business_types) = 'O' THEN 'Private Institution of Higher Education'
       WHEN UPPER(pafa.business_types) = 'P' THEN 'Individual'
       WHEN UPPER(pafa.business_types) = 'Q' THEN 'For-Profit Organization (Other than Small Business)'
       WHEN UPPER(pafa.business_types) = 'R' THEN 'Small Business'
       WHEN UPPER(pafa.business_types) = 'S' THEN 'Hispanic-serving Institution'
       WHEN UPPER(pafa.business_types) = 'T' THEN 'Historically Black Colleges and Universities (HBCUs)'
       WHEN UPPER(pafa.business_types) = 'U' THEN 'Tribally Controlled Colleges and Universities (TCCUs)'
       WHEN UPPER(pafa.business_types) = 'V' THEN 'Alaska Native and Native Hawaiian Serving Institutions'
       WHEN UPPER(pafa.business_types) = 'W' THEN 'Non-domestic (non-US) Entity'
       WHEN UPPER(pafa.business_types) = 'X' THEN 'Other'
       ELSE 'Unknown Types'
   END AS business_types_description,
--    compile_fabs_business_categories(pafa.business_types) AS business_categories,
   pafa.cfda_number AS cfda_number,
   pafa.cfda_title AS cfda_title,
   pafa.sai_number AS sai_number,

   -- recipient data
   pafa.awardee_or_recipient_uniqu AS recipient_unique_id,
   pafa.awardee_or_recipient_legal AS recipient_name,
   NULL::TEXT AS parent_recipient_unique_id,

   -- business categories
   pafa.legal_entity_address_line1 AS recipient_location_address_line1,
   pafa.legal_entity_address_line2 AS recipient_location_address_line2,
   pafa.legal_entity_address_line3 AS recipient_location_address_line3,

   -- foreign province
   pafa.legal_entity_foreign_provi AS recipient_location_foreign_province,
   pafa.legal_entity_foreign_city AS recipient_location_foreign_city_name,
   pafa.legal_entity_foreign_posta AS recipient_location_foreign_postal_code,

   -- country
   pafa.legal_entity_country_code AS recipient_location_country_code,
   pafa.legal_entity_country_name AS recipient_location_country_name,

   -- state
   pafa.legal_entity_state_code AS recipient_location_state_code,
   pafa.legal_entity_state_name AS recipient_location_state_name,

   -- county
   pafa.legal_entity_county_code AS recipient_location_county_code,
   pafa.legal_entity_county_name AS recipient_location_county_name,

   -- city
   pafa.legal_entity_city_code AS recipient_location_city_code,
   pafa.legal_entity_city_name AS recipient_location_city_name,

   -- zip
   pafa.legal_entity_zip5 AS recipient_location_zip5,

   -- congressional disctrict
   pafa.legal_entity_congressional AS recipient_location_congressional_code,

   -- ppop data
   pafa.place_of_performance_code AS pop_code,

   -- foreign
   pafa.place_of_performance_forei AS pop_foreign_province,

   -- country
   pafa.place_of_perform_country_c AS pop_country_code,
   pafa.place_of_perform_country_n AS pop_country_name,

   -- state
   pafa.place_of_perfor_state_code AS pop_state_code,
   pafa.place_of_perform_state_nam AS pop_state_name,

   -- county
   pafa.place_of_perform_county_co AS pop_county_code,
   pafa.place_of_perform_county_na AS pop_county_name,

   -- city
   pafa.place_of_performance_city AS pop_city_name,

   -- zip
   pafa.place_of_performance_zip5 AS pop_zip5,

   -- congressional disctrict
   pafa.place_of_performance_congr AS pop_congressional_code

FROM published_award_financial_assistance AS pafa,

LATERAL aggregate_fabs(
    pafa.awarding_sub_tier_agency_c,
    pafa.fain,
    pafa.uri,
    pafa.record_type
    ) AS fabs_agg(
        awarding_sub_tier_agency_c text,
        fain text,
        uri text,
        total_obligation numeric,
        total_subsidy_cost numeric,
        total_loan_value numeric,
        total_funding_amount numeric,
        date_signed date,
        certified_date date,
        period_of_performance_start_date date,
        period_of_performance_current_end_date date)
WHERE
    pafa.record_type = '1' AND is_active IS TRUE
ORDER BY
    pafa.uri,
    pafa.awarding_sub_tier_agency_c,
    pafa.action_date DESC,
    pafa.award_modification_amendme DESC
);