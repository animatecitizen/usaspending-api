from unittest.mock import MagicMock

from usaspending_api.awards.v2.lookups.lookups import award_type_mapping
from usaspending_api.download.filestreaming import csv_generation


def test_get_csv_sources():
    mappings = csv_generation.VALUE_MAPPINGS

    original = mappings['awards']['filter_function']
    mappings['awards']['filter_function'] = MagicMock(returned_value='')
    csv_sources = csv_generation.get_csv_sources({
        "download_types": ["awards"],
        "filters": {'award_type_codes': list(award_type_mapping.keys())}
    })
    mappings['awards']['filter_function'] = original
    assert len(csv_sources) == 2
    assert csv_sources[0].file_type == 'd1'
    assert csv_sources[0].source_type == 'awards'
    assert csv_sources[1].file_type == 'd2'
    assert csv_sources[1].source_type == 'awards'

    original = mappings['transactions']['filter_function']
    mappings['transactions']['filter_function'] = MagicMock(returned_value='')
    csv_sources = csv_generation.get_csv_sources({
        "download_types": ["transactions"],
        "filters": {'award_type_codes': list(award_type_mapping.keys())}
    })
    mappings['transactions']['filter_function'] = original
    assert len(csv_sources) == 2
    assert csv_sources[0].file_type == 'd1'
    assert csv_sources[0].source_type == 'transactions'
    assert csv_sources[1].file_type == 'd2'
    assert csv_sources[1].source_type == 'transactions'

    original = mappings['sub_awards']['filter_function']
    mappings['sub_awards']['filter_function'] = MagicMock(returned_value='')
    csv_sources = csv_generation.get_csv_sources({
        "download_types": ["sub_awards"],
        "filters": {'award_type_codes': list(award_type_mapping.keys())}
    })
    mappings['sub_awards']['filter_function'] = original
    assert len(csv_sources) == 2
    assert csv_sources[0].file_type == 'd1'
    assert csv_sources[0].source_type == 'sub_awards'
    assert csv_sources[1].file_type == 'd2'
    assert csv_sources[1].source_type == 'sub_awards'

    original = mappings['account_balances']['filter_function']
    mappings['account_balances']['filter_function'] = MagicMock(returned_value='')
    csv_sources = csv_generation.get_csv_sources({
        "download_types": ["account_balances"],
        "account_level": "treasury_account",
        "filters": {}
    })
    mappings['account_balances']['filter_function'] = original
    assert len(csv_sources) == 1
    assert csv_sources[0].file_type == 'treasury_account'
    assert csv_sources[0].source_type == 'account_balances'

    original = mappings['object_class_program_activity']['filter_function']
    mappings['object_class_program_activity']['filter_function'] = MagicMock(returned_value='')
    csv_sources = csv_generation.get_csv_sources({
        "download_types": ["object_class_program_activity"],
        "account_level": "treasury_account",
        "filters": {}
    })
    mappings['object_class_program_activity']['filter_function'] = original
    assert len(csv_sources) == 1
    assert csv_sources[0].file_type == 'treasury_account'
    assert csv_sources[0].source_type == 'object_class_program_activity'


def test_apply_annotations_to_sql():
    sql_string = "SELECT thing_one, thing_two, thing_four, thing_six, thing_eight, CASE WHEN not_thing_three = TRUE " +\
                 "THEN 'L' ELSE NULL END AS alias_thing_three, CASE WHEN not_thing_seven = FALSE THEN 'K' ELSE NULL " +\
                 "END AS alias_thing_seven FROM table_name WHERE thing_one > filter_one AND thing_one < filter_two " +\
                 "AND thing_six = filter_three"
    aliases = ["alias_thing_one", "alias_thing_two", "alias_thing_three", "alias_thing_four", "alias_thing_six",
               "alias_thing_seven", "alias_thing_eight"]

    annotated_sql = csv_generation.apply_annotations_to_sql(sql_string, aliases)
    annotated_sql_string = "SELECT thing_one AS alias_thing_one, thing_two AS alias_thing_two, CASE WHEN " +\
                           "not_thing_three = TRUE THEN 'L' ELSE NULL END AS alias_thing_three, thing_four AS " +\
                           "alias_thing_four, thing_six AS alias_thing_six, CASE WHEN not_thing_seven = FALSE THEN " +\
                           "'K' ELSE NULL END AS alias_thing_seven, thing_eight AS alias_thing_eight FROM " +\
                           "table_name WHERE thing_one > filter_one AND thing_one < filter_two AND thing_six = " +\
                           "filter_three"
    assert annotated_sql_string == annotated_sql
