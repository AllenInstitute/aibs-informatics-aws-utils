from aibs_informatics_core.utils.modules import load_all_modules_from_pkg


def test__imports__work():
    """Test that all modules in gcs_api_interface are importable."""
    import aibs_informatics_aws_utils

    load_all_modules_from_pkg(aibs_informatics_aws_utils)
