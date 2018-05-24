import pytest

from unittest.mock import MagicMock

from usaspending_api.awards.v2.lookups.lookups import award_type_mapping
from usaspending_api.download.filestreaming import csv_generation
from usaspending_api.download.lookups import VALUE_MAPPINGS
from usaspending_api.download.v2.download_column_historical_lookups import query_paths


def test_get_awards_csv_sources():
    original = VALUE_MAPPINGS['awards']['filter_function']
    VALUE_MAPPINGS['awards']['filter_function'] = MagicMock(returned_value='')
    csv_sources = csv_generation.get_csv_sources({
        "download_types": ["awards"],
        "filters": {'award_type_codes': list(award_type_mapping.keys())}
    })
    assert len(csv_sources) == 2
    assert csv_sources[0].file_type == 'd1'
    assert csv_sources[0].source_type == 'awards'
    assert csv_sources[1].file_type == 'd2'
    assert csv_sources[1].source_type == 'awards'


def test_get_transactions_csv_sources():
    original = VALUE_MAPPINGS['transactions']['filter_function']
    VALUE_MAPPINGS['transactions']['filter_function'] = MagicMock(returned_value='')
    csv_sources = csv_generation.get_csv_sources({
        "download_types": ["transactions"],
        "filters": {'award_type_codes': list(award_type_mapping.keys())}
    })
    assert len(csv_sources) == 2
    assert csv_sources[0].file_type == 'd1'
    assert csv_sources[0].source_type == 'transactions'
    assert csv_sources[1].file_type == 'd2'
    assert csv_sources[1].source_type == 'transactions'


def test_get_sub_awards_csv_sources():
    original = VALUE_MAPPINGS['sub_awards']['filter_function']
    VALUE_MAPPINGS['sub_awards']['filter_function'] = MagicMock(returned_value='')
    csv_sources = csv_generation.get_csv_sources({
        "download_types": ["sub_awards"],
        "filters": {'award_type_codes': list(award_type_mapping.keys())}
    })
    assert len(csv_sources) == 2
    assert csv_sources[0].file_type == 'd1'
    assert csv_sources[0].source_type == 'sub_awards'
    assert csv_sources[1].file_type == 'd2'
    assert csv_sources[1].source_type == 'sub_awards'


def test_get_account_balances_csv_sources():
    original = VALUE_MAPPINGS['account_balances']['filter_function']
    VALUE_MAPPINGS['account_balances']['filter_function'] = MagicMock(returned_value='')
    csv_sources = csv_generation.get_csv_sources({
        "download_types": ["account_balances"],
        "account_level": "treasury_account",
        "filters": {}
    })
    assert len(csv_sources) == 1
    assert csv_sources[0].file_type == 'treasury_account'
    assert csv_sources[0].source_type == 'account_balances'


def test_get_object_class_program_activity_csv_sources():
    original = VALUE_MAPPINGS['object_class_program_activity']['filter_function']
    VALUE_MAPPINGS['object_class_program_activity']['filter_function'] = MagicMock(returned_value='')
    csv_sources = csv_generation.get_csv_sources({
        "download_types": ["object_class_program_activity"],
        "account_level": "treasury_account",
        "filters": {}
    })
    assert len(csv_sources) == 1
    assert csv_sources[0].file_type == 'treasury_account'
    assert csv_sources[0].source_type == 'object_class_program_activity'


def test_apply_annotations_to_sql_just_values():
    sql_string = str("SELECT one, two, three, four, five FROM table WHERE six = 'something'")
    aliases = ['alias_one', 'alias_two', 'alias_three', 'alias_four', 'alias_five']

    annotated_sql = csv_generation.apply_annotations_to_sql(sql_string, aliases)

    annotated_string = str("SELECT one AS alias_one, two AS alias_two, three AS alias_three, four AS alias_four, "
                           "five AS alias_five FROM table WHERE six = 'something'")
    assert annotated_sql == annotated_string


def test_apply_annotations_to_sql_just_case():
    sql_string = str("SELECT one, two, four, five, CASE WHEN three = TRUE THEN ‘3’ ELSE NULL END AS alias_three FROM "
                     "table WHERE six = 'something'")
    aliases = ['alias_one', 'alias_two', 'alias_three', 'alias_four', 'alias_five']

    annotated_sql = csv_generation.apply_annotations_to_sql(sql_string, aliases)

    annotated_string = str("SELECT one AS alias_one, two AS alias_two, CASE WHEN three = TRUE THEN ‘3’ ELSE NULL END "
                           "AS alias_three, four AS alias_four, five AS alias_five FROM table WHERE six = 'something'")
    assert annotated_sql == annotated_string


def test_apply_annotations_to_sql_just_concat():
    sql_string = str("SELECT one, two, four, five, CONCAT(three, '-', not_three, '-', yes_three) AS alias_three FROM "
                     "table WHERE six = 'something'")
    aliases = ['alias_one', 'alias_two', 'alias_three', 'alias_four', 'alias_five']

    annotated_sql = csv_generation.apply_annotations_to_sql(sql_string, aliases)

    annotated_string = str("SELECT one AS alias_one, two AS alias_two, CONCAT(three, '-', not_three, '-', yes_three) "
                           "AS alias_three, four AS alias_four, five AS alias_five FROM table WHERE six = 'something'")
    assert annotated_sql == annotated_string


def test_apply_annotations_to_sql_just_multilevel_concat():
    sql_string = str("SELECT one, two, four, five, CONCAT(three, '-', CONCAT(not_three, '-', yes_three)) AS alias_three"
                     " FROM table WHERE six = 'something'")
    aliases = ['alias_one', 'alias_two', 'alias_three', 'alias_four', 'alias_five']

    annotated_sql = csv_generation.apply_annotations_to_sql(sql_string, aliases)

    annotated_string = str("SELECT one AS alias_one, two AS alias_two, CONCAT(three, '-', CONCAT(not_three, '-', "
                           "yes_three)) AS alias_three, four AS alias_four, five AS alias_five FROM table WHERE six = "
                           "'something'")
    assert annotated_sql == annotated_string


def test_apply_annotations_to_sql_just_case_then_concat():
    sql_string = str("SELECT two, four, five, CASE WHEN one = TRUE THEN ‘1’ ELSE NULL END AS alias_one, CONCAT(three, "
                     "'-', not_three, '-', yes_three) AS alias_three FROM table WHERE six = 'something'")
    aliases = ['alias_one', 'alias_two', 'alias_three', 'alias_four', 'alias_five']

    annotated_sql = csv_generation.apply_annotations_to_sql(sql_string, aliases)

    annotated_string = str("SELECT CASE WHEN one = TRUE THEN ‘1’ ELSE NULL END AS alias_one, two AS alias_two, "
                           "CONCAT(three, '-', not_three, '-', yes_three) AS alias_three, four AS alias_four, five AS "
                           "alias_five FROM table WHERE six = 'something'")
    assert annotated_sql == annotated_string


def test_apply_annotations_to_sql_just_concat_then_case():
    sql_string = str("SELECT two, four, five, CONCAT(three, '-', not_three, '-', yes_three) AS alias_three, CASE WHEN "
                     "one = TRUE THEN ‘1’ ELSE NULL END AS alias_one FROM table WHERE six = 'something'")
    aliases = ['alias_one', 'alias_two', 'alias_three', 'alias_four', 'alias_five']

    annotated_sql = csv_generation.apply_annotations_to_sql(sql_string, aliases)

    annotated_string = str("SELECT CASE WHEN one = TRUE THEN ‘1’ ELSE NULL END AS alias_one, two AS alias_two, "
                           "CONCAT(three, '-', not_three, '-', yes_three) AS alias_three, four AS alias_four, five AS "
                           "alias_five FROM table WHERE six = 'something'")
    assert annotated_sql == annotated_string
