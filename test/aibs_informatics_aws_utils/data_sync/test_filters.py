from aibs_informatics_core.models.data_sync import DataSyncFilterConfig

from aibs_informatics_aws_utils.data_sync._filters import extract_filter_patterns


def test__extract_filter_patterns__none_config__returns_none_pair():
    assert extract_filter_patterns(None) == (None, None)


def test__extract_filter_patterns__empty_config__returns_none_pair():
    # An empty config and an absent one filter identically, so they must unpack
    # identically -- otherwise callers would treat "no patterns" as "match nothing".
    include, exclude = extract_filter_patterns(DataSyncFilterConfig())
    assert include is None
    assert exclude is None


def test__extract_filter_patterns__populated_config__returns_compiled_patterns():
    filter_config = DataSyncFilterConfig(include=[r".*\.fastq"], exclude=[r".*\.bam"])
    include, exclude = extract_filter_patterns(filter_config)

    assert include is not None and exclude is not None
    assert [p.pattern for p in include] == [r".*\.fastq"]
    assert [p.pattern for p in exclude] == [r".*\.bam"]


def test__extract_filter_patterns__matches_direct_accessors():
    filter_config = DataSyncFilterConfig(include=[r"sampleA/.*"])
    include, exclude = extract_filter_patterns(filter_config)

    assert include == filter_config.include_patterns
    assert exclude == filter_config.exclude_patterns
