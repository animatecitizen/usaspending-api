DROP FUNCTION IF EXISTS aggregate_fpds(text, text, text, text);

CREATE FUNCTION aggregate_fpds(agency_id_in text, referenced_idv_agency_iden_in text, piid_in text, parent_award_id_in text)
RETURNS RECORD AS $$
DECLARE
    result RECORD;
BEGIN
    SELECT
        agency_id_in AS agency_id,
        referenced_idv_agency_iden_in AS referenced_idv_agency_iden,
        piid_in AS piid,
        parent_award_id_in AS parent_award_id,
        SUM(COALESCE(federal_action_obligation::NUMERIC, 0::NUMERIC)) AS total_obligation,
        SUM(COALESCE(base_and_all_options_value::NUMERIC, 0::NUMERIC)) AS base_and_all_options_value,
        MIN(NULLIF(action_date, '')::DATE) AS date_signed,
        MAX(NULLIF(action_date, '')::DATE) AS certified_date,
        MIN(NULLIF(period_of_performance_star, '')::DATE) AS period_of_performance_start_date,
        MAX(NULLIF(period_of_performance_curr, '')::DATE) AS period_of_performance_current_end_date
    FROM
        detached_award_procurement AS dap
    WHERE
        (dap.piid = piid_in OR (piid_in IS NULL AND dap.piid IS NULL))
        AND
        (dap.parent_award_id = parent_award_id_in OR (parent_award_id_in IS NULL AND dap.parent_award_id IS NULL))
        AND
        (dap.agency_id = agency_id_in OR (agency_id_in IS NULL AND dap.agency_id IS NULL))
        AND
        (dap.referenced_idv_agency_iden = referenced_idv_agency_iden_in OR (referenced_idv_agency_iden_in IS NULL AND dap.referenced_idv_agency_iden IS NULL))
    INTO
        result;
    return result;
END;
$$  LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS aggregate_fabs(text, text, text);

CREATE OR REPLACE FUNCTION aggregate_fabs(awarding_subtier_agency_code_in text, fain_in text, uri_in text, record_type_in text)
RETURNS RECORD AS $$
DECLARE
    result RECORD;
BEGIN
    IF record_type_in = '1' THEN
        SELECT
            awarding_subtier_agency_code_in AS awarding_sub_tier_agency_c,
            fain_in AS fain,
            uri_in AS uri,
            SUM(COALESCE(federal_action_obligation::NUMERIC, 0::NUMERIC)) AS total_obligation,
            SUM(COALESCE(original_loan_subsidy_cost::NUMERIC, 0::NUMERIC)) AS total_subsidy_cost,
            SUM(COALESCE(face_value_loan_guarantee::NUMERIC, 0::NUMERIC)) AS total_loan_value,
            SUM(COALESCE(federal_action_obligation::NUMERIC, 0::NUMERIC) + COALESCE(non_federal_funding_amount::NUMERIC, 0::NUMERIC)) AS total_funding_amount,
            MIN(NULLIF(action_date, '')::DATE) AS date_signed,
            MAX(NULLIF(action_date, '')::DATE) AS certified_date,
            MIN(NULLIF(period_of_performance_star, '')::DATE) AS period_of_performance_start_date,
            MAX(NULLIF(period_of_performance_curr, '')::DATE) AS period_of_performance_current_end_date
        FROM
            published_award_financial_assistance AS faba
        WHERE
            (faba.awarding_sub_tier_agency_c = awarding_subtier_agency_code_in OR (awarding_subtier_agency_code_in IS NULL AND faba.awarding_sub_tier_agency_c IS NULL))
            AND
            (faba.uri = uri_in OR (uri_in IS NULL AND faba.uri IS NULL))
            AND
            faba.record_type = '1'
        INTO
            result;
        return result;
    END IF;

    IF record_type_in IN ('2', '3') THEN
        SELECT
            awarding_subtier_agency_code_in AS awarding_sub_tier_agency_c,
            fain_in AS fain,
            uri_in AS uri,
            SUM(COALESCE(federal_action_obligation::NUMERIC, 0::NUMERIC)) AS total_obligation,
            SUM(COALESCE(original_loan_subsidy_cost::NUMERIC, 0::NUMERIC)) AS total_subsidy_cost,
            SUM(COALESCE(face_value_loan_guarantee::NUMERIC, 0::NUMERIC)) AS total_loan_value,
            SUM(COALESCE(federal_action_obligation::NUMERIC, 0::NUMERIC) + COALESCE(non_federal_funding_amount::NUMERIC, 0::NUMERIC)) AS total_funding_amount,
            MIN(NULLIF(action_date, '')::DATE) AS date_signed,
            MAX(NULLIF(action_date, '')::DATE) AS certified_date,
            MIN(NULLIF(period_of_performance_star, '')::DATE) AS period_of_performance_start_date,
            MAX(NULLIF(period_of_performance_curr, '')::DATE) AS period_of_performance_current_end_date
        FROM
            published_award_financial_assistance AS faba
        WHERE
            (faba.awarding_sub_tier_agency_c = awarding_subtier_agency_code_in OR (awarding_subtier_agency_code_in IS NULL AND faba.awarding_sub_tier_agency_c IS NULL))
            AND
            (faba.fain = fain_in OR (fain_in IS NULL AND faba.fain IS NULL))
            AND
            faba.record_type IN ('2', '3')
        INTO
            result;
        return result;
    END IF;
END;
$$  LANGUAGE plpgsql;

-- FPDS indexes
CREATE INDEX temp_group_fpds_idx ON detached_award_procurement(piid, parent_award_id, agency_id, referenced_idv_agency_iden);
CREATE INDEX temp_group_ordered_fpds_idx ON detached_award_procurement(action_date DESC, award_modification_amendme DESC, transaction_number DESC);

-- FABS indexes
CREATE INDEX temp_group_fabs_idx ON published_award_financial_assistance(awarding_sub_tier_agency_c, fain, uri);
CREATE INDEX temp_group_ordered_fabs_idx ON published_award_financial_assistance(action_date DESC, award_modification_amendme DESC);

DROP TABLE IF EXISTS awards_new;

CREATE TABLE awards_new (
    generated_unique_award_id TEXT,
    type TEXT,
    type_description TEXT,
    agency_id TEXT,
    referenced_idv_agency_iden TEXT,
    referenced_idv_agency_desc TEXT,
    multiple_or_single_award_i TEXT,
    multiple_or_single_aw_desc TEXT,
    type_of_idc TEXT,
    type_of_idc_description TEXT,
    piid TEXT,
    parent_award_piid TEXT,
    fain TEXT,
    uri TEXT,
    total_obligation NUMERIC,
    total_subsidy_cost NUMERIC,
    total_loan_value NUMERIC,
    total_funding_amount NUMERIC,
    awarding_agency_code TEXT,
    awarding_agency_name TEXT,
    awarding_sub_tier_agency_c TEXT,
    awarding_sub_tier_agency_n TEXT,
    awarding_office_code TEXT,
    awarding_office_name TEXT,
    funding_agency_code TEXT,
    funding_agency_name TEXT,
    funding_sub_tier_agency_co TEXT,
    funding_sub_tier_agency_na TEXT,
    funding_office_code TEXT,
    funding_office_name TEXT,
    action_date DATE,
    date_signed DATE,
    description TEXT,
    period_of_performance_start_date DATE,
    period_of_performance_current_end_date DATE,
    potential_total_value_of_award NUMERIC,
    base_and_all_options_value NUMERIC,
    last_modified_date DATE,
    certified_date DATE,
    record_type INTEGER,
    latest_transaction_unique_id TEXT,
--    total_subaward_amount NUMERIC,
--    subaward_count INTEGER,
    pulled_from TEXT,
    product_or_service_code TEXT,
    product_or_service_co_desc TEXT,
    extent_competed TEXT,
    extent_compete_description TEXT,
    type_of_contract_pricing TEXT,
    type_of_contract_pric_desc TEXT,
    contract_award_type_desc TEXT,
    cost_or_pricing_data TEXT,
    cost_or_pricing_data_desc TEXT,
    domestic_or_foreign_entity TEXT,
    domestic_or_foreign_e_desc TEXT,
    fair_opportunity_limited_s TEXT,
    fair_opportunity_limi_desc TEXT,
    foreign_funding TEXT,
    foreign_funding_desc TEXT,
    interagency_contracting_au TEXT,
    interagency_contract_desc TEXT,
    major_program TEXT,
    price_evaluation_adjustmen TEXT,
    program_acronym TEXT,
    subcontracting_plan TEXT,
    subcontracting_plan_desc TEXT,
    multi_year_contract TEXT,
    multi_year_contract_desc TEXT,
    purchase_card_as_payment_m TEXT,
    purchase_card_as_paym_desc TEXT,
    consolidated_contract TEXT,
    consolidated_contract_desc TEXT,
    solicitation_identifier TEXT,
    solicitation_procedures TEXT,
    solicitation_procedur_desc TEXT,
    number_of_offers_received TEXT,
    other_than_full_and_open_c TEXT,
    other_than_full_and_o_desc TEXT,
    commercial_item_acquisitio TEXT,
    commercial_item_acqui_desc TEXT,
    commercial_item_test_progr TEXT,
    commercial_item_test_desc TEXT,
    evaluated_preference TEXT,
    evaluated_preference_desc TEXT,
    fed_biz_opps TEXT,
    fed_biz_opps_description TEXT,
    small_business_competitive TEXT,
    dod_claimant_program_code TEXT,
    dod_claimant_prog_cod_desc TEXT,
    program_system_or_equipmen TEXT,
    program_system_or_equ_desc TEXT,
    information_technology_com TEXT,
    information_technolog_desc TEXT,
    sea_transportation TEXT,
    sea_transportation_desc TEXT,
    clinger_cohen_act_planning TEXT,
    clinger_cohen_act_pla_desc TEXT,
    davis_bacon_act TEXT,
    davis_bacon_act_descrip TEXT,
    service_contract_act TEXT,
    service_contract_act_desc TEXT,
    walsh_healey_act TEXT,
    walsh_healey_act_descrip TEXT,
    naics TEXT,
    naics_description TEXT,
    idv_type TEXT,
    idv_type_description TEXT,
    type_set_aside TEXT,
    type_set_aside_description TEXT,
    assistance_type TEXT,
    business_funds_indicator TEXT,
    business_types TEXT,
    business_types_description TEXT,
--        business_categories TEXT[],
    cfda_number TEXT,
    cfda_title TEXT,
    sai_number TEXT,

    -- recipient data
    recipient_unique_id TEXT, -- DUNS
    recipient_name TEXT,
    parent_recipient_unique_id TEXT,

    -- business categories
    recipient_location_address_line1 TEXT,
    recipient_location_address_line2 TEXT,
    recipient_location_address_line3 TEXT,

    -- foreign province
    recipient_location_foreign_province TEXT,
    recipient_location_foreign_city_name TEXT,
    recipient_location_foreign_postal_code TEXT,

    -- country
    recipient_location_country_code TEXT,
    recipient_location_country_name TEXT,

    -- state
    recipient_location_state_code TEXT,
    recipient_location_state_name TEXT,

    -- county (NONE FOR FPDS)
    recipient_location_county_code TEXT,
    recipient_location_county_name TEXT,

    -- city
    recipient_location_city_code TEXT,
    recipient_location_city_name TEXT,

    -- zip
    recipient_location_zip5 TEXT,

    -- congressional disctrict
    recipient_location_congressional_code TEXT,

    -- ppop data
    pop_code TEXT,

    -- foreign
    pop_foreign_province TEXT,

    -- country
    pop_country_code TEXT,
    pop_country_name TEXT,

    -- state
    pop_state_code TEXT,
    pop_state_name TEXT,

    -- county
    pop_county_code TEXT,
    pop_county_name TEXT,

    -- city
    pop_city_name TEXT,

    -- zip
    pop_zip5 TEXT,

    -- congressional disctrict
    pop_congressional_code TEXT);


INSERT INTO awards_new
SELECT
    DISTINCT ON (dap.piid, dap.parent_award_id, dap.agency_id, dap.referenced_idv_agency_iden)
    'CONT_AW_' ||
        COALESCE(dap_latest.agency_id,'-NONE-') || '_' ||
        COALESCE(dap_latest.referenced_idv_agency_iden,'-NONE-') || '_' ||
        COALESCE(dap_latest.piid,'-NONE-') || '_' ||
        COALESCE(dap_latest.parent_award_id,'-NONE-') AS generated_unique_award_id,
    dap_latest.contract_award_type AS type,
    dap_latest.contract_award_type_desc AS type_description,
    dap_latest.agency_id AS agency_id,
    dap_latest.referenced_idv_agency_iden AS referenced_idv_agency_iden,
    dap_latest.referenced_idv_agency_desc AS referenced_idv_agency_desc,
    dap_latest.multiple_or_single_award_i AS multiple_or_single_award_i,
    dap_latest.multiple_or_single_aw_desc AS multiple_or_single_aw_desc,
    dap_latest.type_of_idc AS type_of_idc,
    dap_latest.type_of_idc_description AS type_of_idc_description,
    dap_latest.piid AS piid,
    dap_latest.parent_award_id AS parent_award_piid,
    NULL::TEXT AS fain,
    NULL::TEXT AS uri,
    fpds_agg.total_obligation AS total_obligation,
    NULL::NUMERIC AS total_subsidy_cost,
    NULL::NUMERIC AS total_loan_value,
    NULL::NUMERIC AS total_funding_amount,
    dap_latest.awarding_agency_code,
    dap_latest.awarding_agency_name,
    dap_latest.awarding_sub_tier_agency_c,
    dap_latest.awarding_sub_tier_agency_n,
    dap_latest.awarding_office_code,
    dap_latest.awarding_office_name,
    dap_latest.funding_agency_code,
    dap_latest.funding_agency_name,
    dap_latest.funding_sub_tier_agency_co AS funding_sub_tier_agency_co,
    dap_latest.funding_sub_tier_agency_na AS funding_sub_tier_agency_na,
    dap_latest.funding_office_code AS funding_office_code,
    dap_latest.funding_office_name AS funding_office_name,
    fpds_agg.certified_date AS action_date,
    fpds_agg.date_signed,
    dap_latest.award_description AS description,
    fpds_agg.period_of_performance_start_date,
    fpds_agg.period_of_performance_current_end_date,
    NULL::NUMERIC AS potential_total_value_of_award,
    fpds_agg.base_and_all_options_value AS base_and_all_options_value,
    dap_latest.last_modified::DATE AS last_modified_date,
    fpds_agg.certified_date,
    NULL::INTEGER AS record_type,
    dap_latest.detached_award_proc_unique AS latest_transaction_unique_id,
--    0 AS total_subaward_amount,
--    0 AS subaward_count,
    dap_latest.pulled_from AS pulled_from,
    dap_latest.product_or_service_code AS product_or_service_code,
    dap_latest.product_or_service_co_desc AS product_or_service_co_desc,
    dap_latest.extent_competed AS extent_competed,
    dap_latest.extent_compete_description AS extent_compete_description,
    dap_latest.type_of_contract_pricing AS type_of_contract_pricing,
    dap_latest.type_of_contract_pric_desc AS type_of_contract_pric_desc,
    dap_latest.contract_award_type_desc AS contract_award_type_desc,
    dap_latest.cost_or_pricing_data AS cost_or_pricing_data,
    dap_latest.cost_or_pricing_data_desc AS cost_or_pricing_data_desc,
    dap_latest.domestic_or_foreign_entity AS domestic_or_foreign_entity,
    dap_latest.domestic_or_foreign_e_desc AS domestic_or_foreign_e_desc,
    dap_latest.fair_opportunity_limited_s AS fair_opportunity_limited_s,
    dap_latest.fair_opportunity_limi_desc AS fair_opportunity_limi_desc,
    dap_latest.foreign_funding AS foreign_funding,
    dap_latest.foreign_funding_desc AS foreign_funding_desc,
    dap_latest.interagency_contracting_au AS interagency_contracting_au,
    dap_latest.interagency_contract_desc AS interagency_contract_desc,
    dap_latest.major_program AS major_program,
    dap_latest.price_evaluation_adjustmen AS price_evaluation_adjustmen,
    dap_latest.program_acronym AS program_acronym,
    dap_latest.subcontracting_plan AS subcontracting_plan,
    dap_latest.subcontracting_plan_desc AS subcontracting_plan_desc,
    dap_latest.multi_year_contract AS multi_year_contract,
    dap_latest.multi_year_contract_desc AS multi_year_contract_desc,
    dap_latest.purchase_card_as_payment_m AS purchase_card_as_payment_m,
    dap_latest.purchase_card_as_paym_desc AS purchase_card_as_paym_desc,
    dap_latest.consolidated_contract AS consolidated_contract,
    dap_latest.consolidated_contract_desc AS consolidated_contract_desc,
    dap_latest.solicitation_identifier AS solicitation_identifier,
    dap_latest.solicitation_procedures AS solicitation_procedures,
    dap_latest.solicitation_procedur_desc AS solicitation_procedur_desc,
    dap_latest.number_of_offers_received AS number_of_offers_received,
    dap_latest.other_than_full_and_open_c AS other_than_full_and_open_c,
    dap_latest.other_than_full_and_o_desc AS other_than_full_and_o_desc,
    dap_latest.commercial_item_acquisitio AS commercial_item_acquisitio,
    dap_latest.commercial_item_acqui_desc AS commercial_item_acqui_desc,
    dap_latest.commercial_item_test_progr AS commercial_item_test_progr,
    dap_latest.commercial_item_test_desc AS commercial_item_test_desc,
    dap_latest.evaluated_preference AS evaluated_preference,
    dap_latest.evaluated_preference_desc AS evaluated_preference_desc,
    dap_latest.fed_biz_opps AS fed_biz_opps,
    dap_latest.fed_biz_opps_description AS fed_biz_opps_description,
    dap_latest.small_business_competitive AS small_business_competitive,
    dap_latest.dod_claimant_program_code AS dod_claimant_program_code,
    dap_latest.dod_claimant_prog_cod_desc AS dod_claimant_prog_cod_desc,
    dap_latest.program_system_or_equipmen AS program_system_or_equipmen,
    dap_latest.program_system_or_equ_desc AS program_system_or_equ_desc,
    dap_latest.information_technology_com AS information_technology_com,
    dap_latest.information_technolog_desc AS information_technolog_desc,
    dap_latest.sea_transportation AS sea_transportation,
    dap_latest.sea_transportation_desc AS sea_transportation_desc,
    dap_latest.clinger_cohen_act_planning AS clinger_cohen_act_planning,
    dap_latest.clinger_cohen_act_pla_desc AS clinger_cohen_act_pla_desc,
    dap_latest.davis_bacon_act AS davis_bacon_act,
    dap_latest.davis_bacon_act_descrip AS davis_bacon_act_descrip,
    dap_latest.service_contract_act AS service_contract_act,
    dap_latest.service_contract_act_desc AS service_contract_act_desc,
    dap_latest.walsh_healey_act AS walsh_healey_act,
    dap_latest.walsh_healey_act_descrip AS walsh_healey_act_descrip,
    dap_latest.naics AS naics,
    dap_latest.naics_description AS naics_description,
    dap_latest.idv_type AS idv_type,
    dap_latest.idv_type_description AS idv_type_description,
    dap_latest.type_set_aside AS type_set_aside,
    dap_latest.type_set_aside_description AS type_set_aside_description,
    NULL::TEXT AS assistance_type,
    NULL::TEXT AS business_funds_indicator,
    NULL::TEXT AS business_types,
    NULL::TEXT AS business_types_description,
    -- business_categories,
    NULL::TEXT AS cfda_number,
    NULL::TEXT AS cfda_title,
    NULL::TEXT AS sai_number,

    -- recipient data
    dap_latest.awardee_or_recipient_uniqu AS recipient_unique_id, -- DUNS
    dap_latest.awardee_or_recipient_legal AS recipient_name,
    dap_latest.ultimate_parent_unique_ide AS parent_recipient_unique_id,

    -- business categories
    dap_latest.legal_entity_address_line1 AS recipient_location_address_line1,
    dap_latest.legal_entity_address_line2 AS recipient_location_address_line2,
    dap_latest.legal_entity_address_line3 AS recipient_location_address_line3,

    -- foreign province
    NULL::TEXT AS recipient_location_foreign_province,
    NULL::TEXT AS recipient_location_foreign_city_name,
    NULL::TEXT AS recipient_location_foreign_city_name,

    -- country
    dap_latest.legal_entity_country_code AS recipient_location_country_code,
    dap_latest.legal_entity_country_name AS recipient_location_country_name,

    -- state
    dap_latest.legal_entity_state_code AS recipient_location_state_code,
    dap_latest.legal_entity_state_descrip AS recipient_location_state_name,

    -- county
    dap_latest.legal_entity_county_code AS recipient_location_county_code,
    dap_latest.legal_entity_county_name AS recipient_location_county_name,

    -- city
    NULL::TEXT AS recipient_location_city_code,
    dap_latest.legal_entity_city_name AS recipient_location_city_name,

    -- zip
    dap_latest.legal_entity_zip5 AS recipient_location_zip5,

    -- congressional disctrict
    dap_latest.legal_entity_congressional AS recipient_location_congressional_code,

    -- ppop data
    NULL::TEXT AS pop_code,

    -- foreign
    NULL::TEXT AS pop_foreign_province,

    -- country
    dap_latest.place_of_perform_country_c AS pop_country_code,
    dap_latest.place_of_perf_country_desc AS pop_country_name,

    -- state
    dap_latest.place_of_performance_state AS pop_state_code,
    dap_latest.place_of_perfor_state_desc AS pop_state_name,

    -- county
    dap_latest.place_of_perform_county_co AS pop_county_code,
    dap_latest.place_of_perform_county_na AS pop_county_name,

    -- city
    dap_latest.place_of_perform_city_name AS pop_city_name,

    -- zip
    dap_latest.place_of_performance_zip5 AS pop_zip5,

    -- congressional disctrict
    dap_latest.place_of_performance_congr AS pop_congressional_code
FROM
    detached_award_procurement AS dap,
LATERAL
    (SELECT *
     FROM detached_award_procurement AS dap_sub
     WHERE
        (dap.piid = dap_sub.piid OR (dap.piid IS NULL AND dap_sub.piid IS NULL))
        AND
        (dap.parent_award_id = dap_sub.parent_award_id OR (dap.parent_award_id IS NULL AND dap_sub.parent_award_id IS NULL))
        AND
        (dap.agency_id = dap_sub.agency_id OR (dap.agency_id IS NULL AND dap_sub.agency_id IS NULL))
        AND
        (dap.referenced_idv_agency_iden = dap_sub.referenced_idv_agency_iden OR (dap.referenced_idv_agency_iden IS NULL AND dap_sub.referenced_idv_agency_iden IS NULL))
     ORDER BY
        dap_sub.action_date DESC,
        dap_sub.award_modification_amendme DESC,
        dap_sub.transaction_number DESC
     LIMIT 1
    ) as dap_latest,

LATERAL aggregate_fpds(dap.agency_id, dap.referenced_idv_agency_iden, dap.piid, dap.parent_award_id)
    AS fpds_agg(
            agency_id TEXT,
            referenced_idv_agency_iden TEXT,
            piid TEXT,
            parent_award_id TEXT,
            total_obligation NUMERIC,
            base_and_all_options_value NUMERIC,
            date_signed DATE,
            certified_date DATE,
            period_of_performance_start_date DATE,
            period_of_performance_current_end_date DATE);

INSERT INTO awards_new
SELECT
   DISTINCT ON (pafa.fain, pafa.awarding_sub_tier_agency_c)
   'ASST_AW_' ||
       COALESCE(pafa_latest.awarding_sub_tier_agency_c,'-NONE-') || '_' ||
       COALESCE(pafa_latest.fain, '-NONE-') || '_' ||
       '-NONE-' AS generated_unique_award_id,
   pafa_latest.assistance_type AS type,
   CASE
       WHEN pafa_latest.assistance_type = '02' THEN 'Block Grant'
       WHEN pafa_latest.assistance_type = '03' THEN 'Formula Grant'
       WHEN pafa_latest.assistance_type = '04' THEN 'Project Grant'
       WHEN pafa_latest.assistance_type = '05' THEN 'Cooperative Agreement'
       WHEN pafa_latest.assistance_type = '06' THEN 'Direct Payment for Specified Use'
       WHEN pafa_latest.assistance_type = '07' THEN 'Direct Loan'
       WHEN pafa_latest.assistance_type = '08' THEN 'Guaranteed/Insured Loan'
       WHEN pafa_latest.assistance_type = '09' THEN 'Insurance'
       WHEN pafa_latest.assistance_type = '10' THEN 'Direct Payment with Unrestricted Use'
       WHEN pafa_latest.assistance_type = '11' THEN 'Other Financial Assistance'
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
   pafa_latest.fain AS fain,
   NULL::TEXT AS uri,
   fabs_agg.total_obligation,
   fabs_agg.total_subsidy_cost,
   fabs_agg.total_loan_value,
   fabs_agg.total_funding_amount,
   pafa_latest.awarding_agency_code AS awarding_agency_code,
   pafa_latest.awarding_agency_name AS awarding_agency_name,
   pafa_latest.awarding_sub_tier_agency_c AS awarding_sub_tier_agency_c,
   pafa_latest.awarding_sub_tier_agency_n AS awarding_sub_tier_agency_n,
   pafa_latest.awarding_office_code AS awarding_office_code,
   pafa_latest.awarding_office_name AS awarding_office_name,
   pafa_latest.funding_agency_code AS funding_agency_code,
   pafa_latest.funding_agency_name AS funding_agency_name,
   pafa_latest.funding_sub_tier_agency_co AS funding_sub_tier_agency_co,
   pafa_latest.funding_sub_tier_agency_na AS funding_sub_tier_agency_na,
   pafa_latest.funding_office_code AS funding_office_code,
   pafa_latest.funding_office_name AS funding_office_name,
   fabs_agg.certified_date as action_date,
   fabs_agg.date_signed,
   pafa_latest.award_description AS description,
   fabs_agg.period_of_performance_start_date,
   fabs_agg.period_of_performance_current_end_date,
   NULL::NUMERIC AS potential_total_value_of_award,
   NULL::NUMERIC AS base_and_all_options_value,
   pafa_latest.modified_at::DATE AS last_modified_date,
   fabs_agg.certified_date,
   pafa_latest.record_type AS record_type,
   pafa_latest.afa_generated_unique AS latest_transaction_unique_id,
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
   pafa_latest.assistance_type AS assistance_type,
   pafa_latest.business_funds_indicator AS business_funds_indicator,
   pafa_latest.business_types AS business_types,
   CASE
       WHEN UPPER(pafa_latest.business_types) = 'A' THEN 'State government'
       WHEN UPPER(pafa_latest.business_types) = 'B' THEN 'County Government'
       WHEN UPPER(pafa_latest.business_types) = 'C' THEN 'City or Township Government'
       WHEN UPPER(pafa_latest.business_types) = 'D' THEN 'Special District Government'
       WHEN UPPER(pafa_latest.business_types) = 'E' THEN 'Regional Organization'
       WHEN UPPER(pafa_latest.business_types) = 'F' THEN 'U.S. Territory or Possession'
       WHEN UPPER(pafa_latest.business_types) = 'G' THEN 'Independent School District'
       WHEN UPPER(pafa_latest.business_types) = 'H' THEN 'Public/State Controlled Institution of Higher Education'
       WHEN UPPER(pafa_latest.business_types) = 'I' THEN 'Indian/Native American Tribal Government (Federally Recognized)'
       WHEN UPPER(pafa_latest.business_types) = 'J' THEN 'Indian/Native American Tribal Government (Other than Federally Recognized)'
       WHEN UPPER(pafa_latest.business_types) = 'K' THEN 'Indian/Native American Tribal Designated Organization'
       WHEN UPPER(pafa_latest.business_types) = 'L' THEN 'Public/Indian Housing Authority'
       WHEN UPPER(pafa_latest.business_types) = 'M' THEN 'Nonprofit with 501(c)(3) IRS Status (Other than Institution of Higher Education)'
       WHEN UPPER(pafa_latest.business_types) = 'N' THEN 'Nonprofit without 501(c)(3) IRS Status (Other than Institution of Higher Education)'
       WHEN UPPER(pafa_latest.business_types) = 'O' THEN 'Private Institution of Higher Education'
       WHEN UPPER(pafa_latest.business_types) = 'P' THEN 'Individual'
       WHEN UPPER(pafa_latest.business_types) = 'Q' THEN 'For-Profit Organization (Other than Small Business)'
       WHEN UPPER(pafa_latest.business_types) = 'R' THEN 'Small Business'
       WHEN UPPER(pafa_latest.business_types) = 'S' THEN 'Hispanic-serving Institution'
       WHEN UPPER(pafa_latest.business_types) = 'T' THEN 'Historically Black Colleges and Universities (HBCUs)'
       WHEN UPPER(pafa_latest.business_types) = 'U' THEN 'Tribally Controlled Colleges and Universities (TCCUs)'
       WHEN UPPER(pafa_latest.business_types) = 'V' THEN 'Alaska Native and Native Hawaiian Serving Institutions'
       WHEN UPPER(pafa_latest.business_types) = 'W' THEN 'Non-domestic (non-US) Entity'
       WHEN UPPER(pafa_latest.business_types) = 'X' THEN 'Other'
       ELSE 'Unknown Types'
   END AS business_types_description,
--    compile_fabs_business_categories(pafa_latest.business_types) AS business_categories,
   pafa_latest.cfda_number AS cfda_number,
   pafa_latest.cfda_title AS cfda_title,
   pafa_latest.sai_number AS sai_number,

   -- recipient data
   pafa_latest.awardee_or_recipient_uniqu AS recipient_unique_id,
   pafa_latest.awardee_or_recipient_legal AS recipient_name,
   NULL::TEXT AS parent_recipient_unique_id,

   -- business categories
   pafa_latest.legal_entity_address_line1 AS recipient_location_address_line1,
   pafa_latest.legal_entity_address_line2 AS recipient_location_address_line2,
   pafa_latest.legal_entity_address_line3 AS recipient_location_address_line3,

   -- foreign province
   pafa_latest.legal_entity_foreign_provi AS recipient_location_foreign_province,
   pafa_latest.legal_entity_foreign_city AS recipient_location_foreign_city_name,
   pafa_latest.legal_entity_foreign_posta AS recipient_location_foreign_postal_code,

   -- country
   pafa_latest.legal_entity_country_code AS recipient_location_country_code,
   pafa_latest.legal_entity_country_name AS recipient_location_country_name,

   -- state
   pafa_latest.legal_entity_state_code AS recipient_location_state_code,
   pafa_latest.legal_entity_state_name AS recipient_location_state_name,

   -- county
   pafa_latest.legal_entity_county_code AS recipient_location_county_code,
   pafa_latest.legal_entity_county_name AS recipient_location_county_name,

   -- city
   pafa_latest.legal_entity_city_code AS recipient_location_city_code,
   pafa_latest.legal_entity_city_name AS recipient_location_city_name,

   -- zip
   pafa_latest.legal_entity_zip5 AS recipient_location_zip5,

   -- congressional disctrict
   pafa_latest.legal_entity_congressional AS recipient_location_congressional_code,

   -- ppop data
   pafa_latest.place_of_performance_code AS pop_code,

   -- foreign
   pafa_latest.place_of_performance_forei AS pop_foreign_province,

   -- country
   pafa_latest.place_of_perform_country_c AS pop_country_code,
   pafa_latest.place_of_perform_country_n AS pop_country_name,

   -- state
   pafa_latest.place_of_perfor_state_code AS pop_state_code,
   pafa_latest.place_of_perform_state_nam AS pop_state_name,

   -- county
   pafa_latest.place_of_perform_county_co AS pop_county_code,
   pafa_latest.place_of_perform_county_na AS pop_county_name,

   -- city
   pafa_latest.place_of_performance_city AS pop_city_name,

   -- zip
   pafa_latest.place_of_performance_zip5 AS pop_zip5,

   -- congressional disctrict
   pafa_latest.place_of_performance_congr AS pop_congressional_code

FROM published_award_financial_assistance AS pafa,
LATERAL
    (SELECT *
     FROM published_award_financial_assistance AS pafa_sub
     WHERE
        (pafa.awarding_sub_tier_agency_c = pafa_sub.awarding_sub_tier_agency_c OR (pafa.awarding_sub_tier_agency_c IS NULL AND pafa_sub.awarding_sub_tier_agency_c IS NULL))
        AND
        (pafa.fain = pafa_sub.fain OR (pafa.fain IS NULL AND pafa_sub.fain IS NULL))
        AND
        pafa_sub.record_type IN ('2', '3')
     ORDER BY
        pafa_sub.action_date DESC,
        pafa_sub.award_modification_amendme DESC
     LIMIT 1
    ) as pafa_latest,

LATERAL aggregate_fpds(pafa.awarding_sub_tier_agency_c, pafa.fain, pafa.uri, pafa.record_type)
    AS fabs_agg(awarding_sub_tier_agency_c text,
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
	pafa.record_type IN ('2', '3')
	AND
	is_active IS TRUE;


INSERT INTO awards_new
SELECT
   DISTINCT ON (pafa.uri, pafa.awarding_sub_tier_agency_c)
   'ASST_AW_' ||
       COALESCE(pafa_latest.awarding_sub_tier_agency_c,'-NONE-') || '_' ||
       '-NONE-' || '_' ||
       COALESCE(pafa_latest.uri, '-NONE-') AS generated_unique_award_id,
   pafa_latest.assistance_type AS type,
   CASE
       WHEN pafa_latest.assistance_type = '02' THEN 'Block Grant'
       WHEN pafa_latest.assistance_type = '03' THEN 'Formula Grant'
       WHEN pafa_latest.assistance_type = '04' THEN 'Project Grant'
       WHEN pafa_latest.assistance_type = '05' THEN 'Cooperative Agreement'
       WHEN pafa_latest.assistance_type = '06' THEN 'Direct Payment for Specified Use'
       WHEN pafa_latest.assistance_type = '07' THEN 'Direct Loan'
       WHEN pafa_latest.assistance_type = '08' THEN 'Guaranteed/Insured Loan'
       WHEN pafa_latest.assistance_type = '09' THEN 'Insurance'
       WHEN pafa_latest.assistance_type = '10' THEN 'Direct Payment with Unrestricted Use'
       WHEN pafa_latest.assistance_type = '11' THEN 'Other Financial Assistance'
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
   pafa_latest.uri AS uri,
   fabs_agg.total_obligation,
   fabs_agg.total_subsidy_cost,
   fabs_agg.total_loan_value,
   fabs_agg.total_funding_amount,
   pafa_latest.awarding_agency_code AS awarding_agency_code,
   pafa_latest.awarding_agency_name AS awarding_agency_name,
   pafa_latest.awarding_sub_tier_agency_c AS awarding_sub_tier_agency_c,
   pafa_latest.awarding_sub_tier_agency_n AS awarding_sub_tier_agency_n,
   pafa_latest.awarding_office_code AS awarding_office_code,
   pafa_latest.awarding_office_name AS awarding_office_name,
   pafa_latest.funding_agency_code AS funding_agency_code,
   pafa_latest.funding_agency_name AS funding_agency_name,
   pafa_latest.funding_sub_tier_agency_co AS funding_sub_tier_agency_co,
   pafa_latest.funding_sub_tier_agency_na AS funding_sub_tier_agency_na,
   pafa_latest.funding_office_code AS funding_office_code,
   pafa_latest.funding_office_name AS funding_office_name,
   fabs_agg.certified_date as action_date,
   fabs_agg.date_signed,
   pafa_latest.award_description AS description,
   fabs_agg.period_of_performance_start_date,
   fabs_agg.period_of_performance_current_end_date,
   NULL::NUMERIC AS potential_total_value_of_award,
   NULL::NUMERIC AS base_and_all_options_value,
   pafa_latest.modified_at::DATE AS last_modified_date,
   fabs_agg.certified_date,
   pafa_latest.record_type AS record_type,
   pafa_latest.afa_generated_unique AS latest_transaction_unique_id,
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
   pafa_latest.assistance_type AS assistance_type,
   pafa_latest.business_funds_indicator AS business_funds_indicator,
   pafa_latest.business_types AS business_types,
   CASE
       WHEN UPPER(pafa_latest.business_types) = 'A' THEN 'State government'
       WHEN UPPER(pafa_latest.business_types) = 'B' THEN 'County Government'
       WHEN UPPER(pafa_latest.business_types) = 'C' THEN 'City or Township Government'
       WHEN UPPER(pafa_latest.business_types) = 'D' THEN 'Special District Government'
       WHEN UPPER(pafa_latest.business_types) = 'E' THEN 'Regional Organization'
       WHEN UPPER(pafa_latest.business_types) = 'F' THEN 'U.S. Territory or Possession'
       WHEN UPPER(pafa_latest.business_types) = 'G' THEN 'Independent School District'
       WHEN UPPER(pafa_latest.business_types) = 'H' THEN 'Public/State Controlled Institution of Higher Education'
       WHEN UPPER(pafa_latest.business_types) = 'I' THEN 'Indian/Native American Tribal Government (Federally Recognized)'
       WHEN UPPER(pafa_latest.business_types) = 'J' THEN 'Indian/Native American Tribal Government (Other than Federally Recognized)'
       WHEN UPPER(pafa_latest.business_types) = 'K' THEN 'Indian/Native American Tribal Designated Organization'
       WHEN UPPER(pafa_latest.business_types) = 'L' THEN 'Public/Indian Housing Authority'
       WHEN UPPER(pafa_latest.business_types) = 'M' THEN 'Nonprofit with 501(c)(3) IRS Status (Other than Institution of Higher Education)'
       WHEN UPPER(pafa_latest.business_types) = 'N' THEN 'Nonprofit without 501(c)(3) IRS Status (Other than Institution of Higher Education)'
       WHEN UPPER(pafa_latest.business_types) = 'O' THEN 'Private Institution of Higher Education'
       WHEN UPPER(pafa_latest.business_types) = 'P' THEN 'Individual'
       WHEN UPPER(pafa_latest.business_types) = 'Q' THEN 'For-Profit Organization (Other than Small Business)'
       WHEN UPPER(pafa_latest.business_types) = 'R' THEN 'Small Business'
       WHEN UPPER(pafa_latest.business_types) = 'S' THEN 'Hispanic-serving Institution'
       WHEN UPPER(pafa_latest.business_types) = 'T' THEN 'Historically Black Colleges and Universities (HBCUs)'
       WHEN UPPER(pafa_latest.business_types) = 'U' THEN 'Tribally Controlled Colleges and Universities (TCCUs)'
       WHEN UPPER(pafa_latest.business_types) = 'V' THEN 'Alaska Native and Native Hawaiian Serving Institutions'
       WHEN UPPER(pafa_latest.business_types) = 'W' THEN 'Non-domestic (non-US) Entity'
       WHEN UPPER(pafa_latest.business_types) = 'X' THEN 'Other'
       ELSE 'Unknown Types'
   END AS business_types_description,
--    compile_fabs_business_categories(pafa_latest.business_types) AS business_categories,
   pafa_latest.cfda_number AS cfda_number,
   pafa_latest.cfda_title AS cfda_title,
   pafa_latest.sai_number AS sai_number,

   -- recipient data
   pafa_latest.awardee_or_recipient_uniqu AS recipient_unique_id,
   pafa_latest.awardee_or_recipient_legal AS recipient_name,
   NULL::TEXT AS parent_recipient_unique_id,

   -- business categories
   pafa_latest.legal_entity_address_line1 AS recipient_location_address_line1,
   pafa_latest.legal_entity_address_line2 AS recipient_location_address_line2,
   pafa_latest.legal_entity_address_line3 AS recipient_location_address_line3,

   -- foreign province
   pafa_latest.legal_entity_foreign_provi AS recipient_location_foreign_province,
   pafa_latest.legal_entity_foreign_city AS recipient_location_foreign_city_name,
   pafa_latest.legal_entity_foreign_posta AS recipient_location_foreign_postal_code,

   -- country
   pafa_latest.legal_entity_country_code AS recipient_location_country_code,
   pafa_latest.legal_entity_country_name AS recipient_location_country_name,

   -- state
   pafa_latest.legal_entity_state_code AS recipient_location_state_code,
   pafa_latest.legal_entity_state_name AS recipient_location_state_name,

   -- county
   pafa_latest.legal_entity_county_code AS recipient_location_county_code,
   pafa_latest.legal_entity_county_name AS recipient_location_county_name,

   -- city
   pafa_latest.legal_entity_city_code AS recipient_location_city_code,
   pafa_latest.legal_entity_city_name AS recipient_location_city_name,

   -- zip
   pafa_latest.legal_entity_zip5 AS recipient_location_zip5,

   -- congressional disctrict
   pafa_latest.legal_entity_congressional AS recipient_location_congressional_code,

   -- ppop data
   pafa_latest.place_of_performance_code AS pop_code,

   -- foreign
   pafa_latest.place_of_performance_forei AS pop_foreign_province,

   -- country
   pafa_latest.place_of_perform_country_c AS pop_country_code,
   pafa_latest.place_of_perform_country_n AS pop_country_name,

   -- state
   pafa_latest.place_of_perfor_state_code AS pop_state_code,
   pafa_latest.place_of_perform_state_nam AS pop_state_name,

   -- county
   pafa_latest.place_of_perform_county_co AS pop_county_code,
   pafa_latest.place_of_perform_county_na AS pop_county_name,

   -- city
   pafa_latest.place_of_performance_city AS pop_city_name,

   -- zip
   pafa_latest.place_of_performance_zip5 AS pop_zip5,

   -- congressional disctrict
   pafa_latest.place_of_performance_congr AS pop_congressional_code

FROM published_award_financial_assistance AS pafa,
LATERAL
    (SELECT *
     FROM published_award_financial_assistance AS pafa_sub
     WHERE
        (pafa.awarding_sub_tier_agency_c = pafa_sub.awarding_sub_tier_agency_c OR (pafa.awarding_sub_tier_agency_c IS NULL AND pafa_sub.awarding_sub_tier_agency_c IS NULL))
        AND
        (pafa.uri = pafa_sub.uri OR (pafa.uri IS NULL AND pafa_sub.uri IS NULL))
        AND
        pafa_sub.record_type = '1'
     ORDER BY
        pafa_sub.action_date DESC,
        pafa_sub.award_modification_amendme DESC
     LIMIT 1
    ) as pafa_latest,

LATERAL aggregate_fpds(pafa.awarding_sub_tier_agency_c, pafa.fain, pafa.uri)
    AS fabs_agg(awarding_sub_tier_agency_c text,
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
	pafa.record_type = '1'
	AND
	is_active IS TRUE;