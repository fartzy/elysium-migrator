from pathlib import Path

import pytest


def test_vertica_odbc(vertica_conn):

    df = pd.read_sql(
        """
        SELECT * 
        FROM Elysium.FINSECMM_Busts_T
        LIMIT 500
        """,
        vertica_conn.ctx,
    )

    rows = len(df.index)

    assert rows == 500


def test_coordinator_yaml_load(sample_config_file):
    err = Coordinator.export(sample_config_file)
    assert err == None
