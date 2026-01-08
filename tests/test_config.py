def test_config_import():
    import utils.config

    assert utils.config.catalog == "pei_assessment_catalog"