-- CREATE INDEX detached_award_proc_ordered_idx ON detached_award_procurement USING BTREE(
--     dap.piid,
--     dap.parent_award_id,
--     dap.agency_id,
--     dap.referenced_idv_agency_iden,
--     dap.action_date DESC,
--     dap.award_modification_amendme DESC,
--     dap.transaction_number DESC);

-- ANALYZE VERBOSE detached_award_procurement;

CREATE MATERIALIZED VIEW testing_awards_view AS
SELECT
    DISTINCT ON (dap.piid, dap.parent_award_id, dap.agency_id, dap.referenced_idv_agency_iden)
    'CONT_AW_' ||
        COALESCE(dap.agency_id,'-NONE-') || '_' ||
        COALESCE(dap.referenced_idv_agency_iden,'-NONE-') || '_' ||
        COALESCE(dap.piid,'-NONE-') || '_' ||
        COALESCE(dap.parent_award_id,'-NONE-') AS generated_unique_award_id,
    dap.contract_award_type AS type,
    dap.contract_award_type_desc AS type_description,
    dap.agency_id AS agency_id,
    dap.referenced_idv_agency_iden AS referenced_idv_agency_iden,
    dap.referenced_idv_agency_desc AS referenced_idv_agency_desc,
    dap.multiple_or_single_award_i AS multiple_or_single_award_i,
    dap.multiple_or_single_aw_desc AS multiple_or_single_aw_desc,
    dap.type_of_idc AS type_of_idc,
    dap.type_of_idc_description AS type_of_idc_description,
    dap.piid AS piid,
    dap.parent_award_id AS parent_award_piid,
    NULL::TEXT AS fain,
    NULL::TEXT AS uri,
    fpds_agg.total_obligation AS total_obligation,
    NULL::NUMERIC AS total_subsidy_cost,
    NULL::NUMERIC AS total_loan_value,
    NULL::NUMERIC AS total_funding_amount,
    dap.awarding_agency_code,
    dap.awarding_agency_name,
    dap.awarding_sub_tier_agency_c,
    dap.awarding_sub_tier_agency_n,
    dap.awarding_office_code,
    dap.awarding_office_name,
    dap.funding_agency_code,
    dap.funding_agency_name,
    dap.funding_sub_tier_agency_co AS funding_sub_tier_agency_co,
    dap.funding_sub_tier_agency_na AS funding_sub_tier_agency_na,
    dap.funding_office_code AS funding_office_code,
    dap.funding_office_name AS funding_office_name,
    fpds_agg.certified_date AS action_date,
    fpds_agg.date_signed,
    dap.award_description AS description,
    fpds_agg.period_of_performance_start_date,
    fpds_agg.period_of_performance_current_end_date,
    NULL::NUMERIC AS potential_total_value_of_award,
    fpds_agg.base_and_all_options_value AS base_and_all_options_value,
    dap.last_modified::DATE AS last_modified_date,
    fpds_agg.certified_date,
    NULL::INTEGER AS record_type,
    dap.detached_award_proc_unique AS latest_transaction_unique_id,
--    0 AS total_subaward_amount,
--    0 AS subaward_count,
    dap.pulled_from AS pulled_from,
    dap.product_or_service_code AS product_or_service_code,
    dap.product_or_service_co_desc AS product_or_service_co_desc,
    dap.extent_competed AS extent_competed,
    dap.extent_compete_description AS extent_compete_description,
    dap.type_of_contract_pricing AS type_of_contract_pricing,
    dap.type_of_contract_pric_desc AS type_of_contract_pric_desc,
    dap.contract_award_type_desc AS contract_award_type_desc,
    dap.cost_or_pricing_data AS cost_or_pricing_data,
    dap.cost_or_pricing_data_desc AS cost_or_pricing_data_desc,
    dap.domestic_or_foreign_entity AS domestic_or_foreign_entity,
    dap.domestic_or_foreign_e_desc AS domestic_or_foreign_e_desc,
    dap.fair_opportunity_limited_s AS fair_opportunity_limited_s,
    dap.fair_opportunity_limi_desc AS fair_opportunity_limi_desc,
    dap.foreign_funding AS foreign_funding,
    dap.foreign_funding_desc AS foreign_funding_desc,
    dap.interagency_contracting_au AS interagency_contracting_au,
    dap.interagency_contract_desc AS interagency_contract_desc,
    dap.major_program AS major_program,
    dap.price_evaluation_adjustmen AS price_evaluation_adjustmen,
    dap.program_acronym AS program_acronym,
    dap.subcontracting_plan AS subcontracting_plan,
    dap.subcontracting_plan_desc AS subcontracting_plan_desc,
    dap.multi_year_contract AS multi_year_contract,
    dap.multi_year_contract_desc AS multi_year_contract_desc,
    dap.purchase_card_as_payment_m AS purchase_card_as_payment_m,
    dap.purchase_card_as_paym_desc AS purchase_card_as_paym_desc,
    dap.consolidated_contract AS consolidated_contract,
    dap.consolidated_contract_desc AS consolidated_contract_desc,
    dap.solicitation_identifier AS solicitation_identifier,
    dap.solicitation_procedures AS solicitation_procedures,
    dap.solicitation_procedur_desc AS solicitation_procedur_desc,
    dap.number_of_offers_received AS number_of_offers_received,
    dap.other_than_full_and_open_c AS other_than_full_and_open_c,
    dap.other_than_full_and_o_desc AS other_than_full_and_o_desc,
    dap.commercial_item_acquisitio AS commercial_item_acquisitio,
    dap.commercial_item_acqui_desc AS commercial_item_acqui_desc,
    dap.commercial_item_test_progr AS commercial_item_test_progr,
    dap.commercial_item_test_desc AS commercial_item_test_desc,
    dap.evaluated_preference AS evaluated_preference,
    dap.evaluated_preference_desc AS evaluated_preference_desc,
    dap.fed_biz_opps AS fed_biz_opps,
    dap.fed_biz_opps_description AS fed_biz_opps_description,
    dap.small_business_competitive AS small_business_competitive,
    dap.dod_claimant_program_code AS dod_claimant_program_code,
    dap.dod_claimant_prog_cod_desc AS dod_claimant_prog_cod_desc,
    dap.program_system_or_equipmen AS program_system_or_equipmen,
    dap.program_system_or_equ_desc AS program_system_or_equ_desc,
    dap.information_technology_com AS information_technology_com,
    dap.information_technolog_desc AS information_technolog_desc,
    dap.sea_transportation AS sea_transportation,
    dap.sea_transportation_desc AS sea_transportation_desc,
    dap.clinger_cohen_act_planning AS clinger_cohen_act_planning,
    dap.clinger_cohen_act_pla_desc AS clinger_cohen_act_pla_desc,
    dap.davis_bacon_act AS davis_bacon_act,
    dap.davis_bacon_act_descrip AS davis_bacon_act_descrip,
    dap.service_contract_act AS service_contract_act,
    dap.service_contract_act_desc AS service_contract_act_desc,
    dap.walsh_healey_act AS walsh_healey_act,
    dap.walsh_healey_act_descrip AS walsh_healey_act_descrip,
    dap.naics AS naics,
    dap.naics_description AS naics_description,
    dap.idv_type AS idv_type,
    dap.idv_type_description AS idv_type_description,
    dap.type_set_aside AS type_set_aside,
    dap.type_set_aside_description AS type_set_aside_description,
    NULL::TEXT AS assistance_type,
    NULL::TEXT AS business_funds_indicator,
    NULL::TEXT AS business_types,
    NULL::TEXT AS business_types_description,
    -- business_categories,
    NULL::TEXT AS cfda_number,
    NULL::TEXT AS cfda_title,
    NULL::TEXT AS sai_number,

    -- recipient data
    dap.awardee_or_recipient_uniqu AS recipient_unique_id, -- DUNS
    dap.awardee_or_recipient_legal AS recipient_name,
    dap.ultimate_parent_unique_ide AS parent_recipient_unique_id,

    -- business categories
    dap.legal_entity_address_line1 AS recipient_location_address_line1,
    dap.legal_entity_address_line2 AS recipient_location_address_line2,
    dap.legal_entity_address_line3 AS recipient_location_address_line3,

    -- foreign province
    NULL::TEXT AS recipient_location_foreign_province,
    NULL::TEXT AS recipient_location_foreign_city_name,
    NULL::TEXT AS recipient_location_foreign_postal_code,

    -- country
    dap.legal_entity_country_code AS recipient_location_country_code,
    dap.legal_entity_country_name AS recipient_location_country_name,

    -- state
    dap.legal_entity_state_code AS recipient_location_state_code,
    dap.legal_entity_state_descrip AS recipient_location_state_name,

    -- county
    dap.legal_entity_county_code AS recipient_location_county_code,
    dap.legal_entity_county_name AS recipient_location_county_name,

    -- city
    NULL::TEXT AS recipient_location_city_code,
    dap.legal_entity_city_name AS recipient_location_city_name,

    -- zip
    dap.legal_entity_zip5 AS recipient_location_zip5,

    -- congressional disctrict
    dap.legal_entity_congressional AS recipient_location_congressional_code,

    -- ppop data
    NULL::TEXT AS pop_code,

    -- foreign
    NULL::TEXT AS pop_foreign_province,

    -- country
    dap.place_of_perform_country_c AS pop_country_code,
    dap.place_of_perf_country_desc AS pop_country_name,

    -- state
    dap.place_of_performance_state AS pop_state_code,
    dap.place_of_perfor_state_desc AS pop_state_name,

    -- county
    dap.place_of_perform_county_co AS pop_county_code,
    dap.place_of_perform_county_na AS pop_county_name,

    -- city
    dap.place_of_perform_city_name AS pop_city_name,

    -- zip
    dap.place_of_performance_zip5 AS pop_zip5,

    -- congressional disctrict
    dap.place_of_performance_congr AS pop_congressional_code
FROM
    detached_award_procurement AS dap,
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
            period_of_performance_current_end_date DATE)
ORDER BY
    dap.piid,
    dap.parent_award_id,
    dap.agency_id,
    dap.referenced_idv_agency_iden,
    dap.action_date DESC,
    dap.award_modification_amendme DESC,
    dap.transaction_number DESC;

-- WITH dap_latest AS
--     (SELECT *
--      FROM detached_award_procurement AS dap_sub
--      WHERE
--         (dap.piid = dap_sub.piid OR (dap.piid IS NULL AND dap_sub.piid IS NULL))
--         AND
--         (dap.parent_award_id = dap_sub.parent_award_id OR (dap.parent_award_id IS NULL AND dap_sub.parent_award_id IS NULL))
--         AND
--         (dap.agency_id = dap_sub.agency_id OR (dap.agency_id IS NULL AND dap_sub.agency_id IS NULL))
--         AND
--         (dap.referenced_idv_agency_iden = dap_sub.referenced_idv_agency_iden OR (dap.referenced_idv_agency_iden IS NULL AND dap_sub.referenced_idv_agency_iden IS NULL))
--      ORDER BY
--         dap_sub.action_date DESC,
--         dap_sub.award_modification_amendme DESC,
--         dap_sub.transaction_number DESC
--      LIMIT 1
--     )


-- INSERT INTO awards_new
-- SELECT
--     DISTINCT ON (dap.piid, dap.parent_award_id, dap.agency_id, dap.referenced_idv_agency_iden)
--     'CONT_AW_' ||
--         COALESCE(dap_latest.agency_id,'-NONE-') || '_' ||
--         COALESCE(dap_latest.referenced_idv_agency_iden,'-NONE-') || '_' ||
--         COALESCE(dap_latest.piid,'-NONE-') || '_' ||
--         COALESCE(dap_latest.parent_award_id,'-NONE-') AS generated_unique_award_id,
--     dap_latest.contract_award_type AS type,
--     dap_latest.contract_award_type_desc AS type_description,
--     dap_latest.agency_id AS agency_id,
--     dap_latest.referenced_idv_agency_iden AS referenced_idv_agency_iden,
--     dap_latest.referenced_idv_agency_desc AS referenced_idv_agency_desc,
--     dap_latest.multiple_or_single_award_i AS multiple_or_single_award_i,
--     dap_latest.multiple_or_single_aw_desc AS multiple_or_single_aw_desc,
--     dap_latest.type_of_idc AS type_of_idc,
--     dap_latest.type_of_idc_description AS type_of_idc_description,
--     dap_latest.piid AS piid,
--     dap_latest.parent_award_id AS parent_award_piid,
--     NULL::TEXT AS fain,
--     NULL::TEXT AS uri,
--     fpds_agg.total_obligation AS total_obligation,
--     NULL::NUMERIC AS total_subsidy_cost,
--     NULL::NUMERIC AS total_loan_value,
--     NULL::NUMERIC AS total_funding_amount,
--     dap_latest.awarding_agency_code,
--     dap_latest.awarding_agency_name,
--     dap_latest.awarding_sub_tier_agency_c,
--     dap_latest.awarding_sub_tier_agency_n,
--     dap_latest.awarding_office_code,
--     dap_latest.awarding_office_name,
--     dap_latest.funding_agency_code,
--     dap_latest.funding_agency_name,
--     dap_latest.funding_sub_tier_agency_co AS funding_sub_tier_agency_co,
--     dap_latest.funding_sub_tier_agency_na AS funding_sub_tier_agency_na,
--     dap_latest.funding_office_code AS funding_office_code,
--     dap_latest.funding_office_name AS funding_office_name,
--     fpds_agg.certified_date AS action_date,
--     fpds_agg.date_signed,
--     dap_latest.award_description AS description,
--     fpds_agg.period_of_performance_start_date,
--     fpds_agg.period_of_performance_current_end_date,
--     NULL::NUMERIC AS potential_total_value_of_award,
--     fpds_agg.base_and_all_options_value AS base_and_all_options_value,
--     dap_latest.last_modified::DATE AS last_modified_date,
--     fpds_agg.certified_date,
--     NULL::INTEGER AS record_type,
--     dap_latest.detached_award_proc_unique AS latest_transaction_unique_id,
-- --    0 AS total_subaward_amount,
-- --    0 AS subaward_count,
--     dap_latest.pulled_from AS pulled_from,
--     dap_latest.product_or_service_code AS product_or_service_code,
--     dap_latest.product_or_service_co_desc AS product_or_service_co_desc,
--     dap_latest.extent_competed AS extent_competed,
--     dap_latest.extent_compete_description AS extent_compete_description,
--     dap_latest.type_of_contract_pricing AS type_of_contract_pricing,
--     dap_latest.type_of_contract_pric_desc AS type_of_contract_pric_desc,
--     dap_latest.contract_award_type_desc AS contract_award_type_desc,
--     dap_latest.cost_or_pricing_data AS cost_or_pricing_data,
--     dap_latest.cost_or_pricing_data_desc AS cost_or_pricing_data_desc,
--     dap_latest.domestic_or_foreign_entity AS domestic_or_foreign_entity,
--     dap_latest.domestic_or_foreign_e_desc AS domestic_or_foreign_e_desc,
--     dap_latest.fair_opportunity_limited_s AS fair_opportunity_limited_s,
--     dap_latest.fair_opportunity_limi_desc AS fair_opportunity_limi_desc,
--     dap_latest.foreign_funding AS foreign_funding,
--     dap_latest.foreign_funding_desc AS foreign_funding_desc,
--     dap_latest.interagency_contracting_au AS interagency_contracting_au,
--     dap_latest.interagency_contract_desc AS interagency_contract_desc,
--     dap_latest.major_program AS major_program,
--     dap_latest.price_evaluation_adjustmen AS price_evaluation_adjustmen,
--     dap_latest.program_acronym AS program_acronym,
--     dap_latest.subcontracting_plan AS subcontracting_plan,
--     dap_latest.subcontracting_plan_desc AS subcontracting_plan_desc,
--     dap_latest.multi_year_contract AS multi_year_contract,
--     dap_latest.multi_year_contract_desc AS multi_year_contract_desc,
--     dap_latest.purchase_card_as_payment_m AS purchase_card_as_payment_m,
--     dap_latest.purchase_card_as_paym_desc AS purchase_card_as_paym_desc,
--     dap_latest.consolidated_contract AS consolidated_contract,
--     dap_latest.consolidated_contract_desc AS consolidated_contract_desc,
--     dap_latest.solicitation_identifier AS solicitation_identifier,
--     dap_latest.solicitation_procedures AS solicitation_procedures,
--     dap_latest.solicitation_procedur_desc AS solicitation_procedur_desc,
--     dap_latest.number_of_offers_received AS number_of_offers_received,
--     dap_latest.other_than_full_and_open_c AS other_than_full_and_open_c,
--     dap_latest.other_than_full_and_o_desc AS other_than_full_and_o_desc,
--     dap_latest.commercial_item_acquisitio AS commercial_item_acquisitio,
--     dap_latest.commercial_item_acqui_desc AS commercial_item_acqui_desc,
--     dap_latest.commercial_item_test_progr AS commercial_item_test_progr,
--     dap_latest.commercial_item_test_desc AS commercial_item_test_desc,
--     dap_latest.evaluated_preference AS evaluated_preference,
--     dap_latest.evaluated_preference_desc AS evaluated_preference_desc,
--     dap_latest.fed_biz_opps AS fed_biz_opps,
--     dap_latest.fed_biz_opps_description AS fed_biz_opps_description,
--     dap_latest.small_business_competitive AS small_business_competitive,
--     dap_latest.dod_claimant_program_code AS dod_claimant_program_code,
--     dap_latest.dod_claimant_prog_cod_desc AS dod_claimant_prog_cod_desc,
--     dap_latest.program_system_or_equipmen AS program_system_or_equipmen,
--     dap_latest.program_system_or_equ_desc AS program_system_or_equ_desc,
--     dap_latest.information_technology_com AS information_technology_com,
--     dap_latest.information_technolog_desc AS information_technolog_desc,
--     dap_latest.sea_transportation AS sea_transportation,
--     dap_latest.sea_transportation_desc AS sea_transportation_desc,
--     dap_latest.clinger_cohen_act_planning AS clinger_cohen_act_planning,
--     dap_latest.clinger_cohen_act_pla_desc AS clinger_cohen_act_pla_desc,
--     dap_latest.davis_bacon_act AS davis_bacon_act,
--     dap_latest.davis_bacon_act_descrip AS davis_bacon_act_descrip,
--     dap_latest.service_contract_act AS service_contract_act,
--     dap_latest.service_contract_act_desc AS service_contract_act_desc,
--     dap_latest.walsh_healey_act AS walsh_healey_act,
--     dap_latest.walsh_healey_act_descrip AS walsh_healey_act_descrip,
--     dap_latest.naics AS naics,
--     dap_latest.naics_description AS naics_description,
--     dap_latest.idv_type AS idv_type,
--     dap_latest.idv_type_description AS idv_type_description,
--     dap_latest.type_set_aside AS type_set_aside,
--     dap_latest.type_set_aside_description AS type_set_aside_description,
--     NULL::TEXT AS assistance_type,
--     NULL::TEXT AS business_funds_indicator,
--     NULL::TEXT AS business_types,
--     NULL::TEXT AS business_types_description,
--     -- business_categories,
--     NULL::TEXT AS cfda_number,
--     NULL::TEXT AS cfda_title,
--     NULL::TEXT AS sai_number,

--     -- recipient data
--     dap_latest.awardee_or_recipient_uniqu AS recipient_unique_id, -- DUNS
--     dap_latest.awardee_or_recipient_legal AS recipient_name,
--     dap_latest.ultimate_parent_unique_ide AS parent_recipient_unique_id,

--     -- business categories
--     dap_latest.legal_entity_address_line1 AS recipient_location_address_line1,
--     dap_latest.legal_entity_address_line2 AS recipient_location_address_line2,
--     dap_latest.legal_entity_address_line3 AS recipient_location_address_line3,

--     -- foreign province
--     NULL::TEXT AS recipient_location_foreign_province,
--     NULL::TEXT AS recipient_location_foreign_city_name,
--     NULL::TEXT AS recipient_location_foreign_city_name,

--     -- country
--     dap_latest.legal_entity_country_code AS recipient_location_country_code,
--     dap_latest.legal_entity_country_name AS recipient_location_country_name,

--     -- state
--     dap_latest.legal_entity_state_code AS recipient_location_state_code,
--     dap_latest.legal_entity_state_descrip AS recipient_location_state_name,

--     -- county
--     dap_latest.legal_entity_county_code AS recipient_location_county_code,
--     dap_latest.legal_entity_county_name AS recipient_location_county_name,

--     -- city
--     NULL::TEXT AS recipient_location_city_code,
--     dap_latest.legal_entity_city_name AS recipient_location_city_name,

--     -- zip
--     dap_latest.legal_entity_zip5 AS recipient_location_zip5,

--     -- congressional disctrict
--     dap_latest.legal_entity_congressional AS recipient_location_congressional_code,

--     -- ppop data
--     NULL::TEXT AS pop_code,

--     -- foreign
--     NULL::TEXT AS pop_foreign_province,

--     -- country
--     dap_latest.place_of_perform_country_c AS pop_country_code,
--     dap_latest.place_of_perf_country_desc AS pop_country_name,

--     -- state
--     dap_latest.place_of_performance_state AS pop_state_code,
--     dap_latest.place_of_perfor_state_desc AS pop_state_name,

--     -- county
--     dap_latest.place_of_perform_county_co AS pop_county_code,
--     dap_latest.place_of_perform_county_na AS pop_county_name,

--     -- city
--     dap_latest.place_of_perform_city_name AS pop_city_name,

--     -- zip
--     dap_latest.place_of_performance_zip5 AS pop_zip5,

--     -- congressional disctrict
--     dap_latest.place_of_performance_congr AS pop_congressional_code
-- FROM
--     detached_award_procurement AS dap,

-- LATERAL aggregate_fpds(dap.agency_id, dap.referenced_idv_agency_iden, dap.piid, dap.parent_award_id)
--     AS fpds_agg(
--             agency_id TEXT,
--             referenced_idv_agency_iden TEXT,
--             piid TEXT,
--             parent_award_id TEXT,
--             total_obligation NUMERIC,
--             base_and_all_options_value NUMERIC,
--             date_signed DATE,
--             certified_date DATE,
--             period_of_performance_start_date DATE,
--             period_of_performance_current_end_date DATE);