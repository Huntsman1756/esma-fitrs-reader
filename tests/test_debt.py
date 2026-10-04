import json
import zipfile
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from esma_fitrs.debt import SCHEMA, instrument, records

ISIN = "XS3373438483"


def test_sync_is_atomic_when_a_source_has_no_recognised_records(tmp_path, monkeypatch):
    from esma_fitrs import debt as module

    archive_path = tmp_path / "bond-transparency-raw/test.zip"
    archive_path.parent.mkdir(parents=True)
    source = {
        "file_name": "test.zip",
        "creation_date": "2026-10-03",
        "download_link": "https://fitrs.esma.europa.eu/test.zip",
    }
    monkeypatch.setattr(
        module, "catalogue", lambda kind, *args: [source] if kind == "FULNCR" else []
    )
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr(
            "data.xml",
            f"<Document><NonEqtyTrnsprncyData>"
            f"<Id><ISINAndSubClss><ISIN>{ISIN}</ISIN>"
            "</ISINAndSubClss></Id></NonEqtyTrnsprncyData></Document>",
        )
    assert module.sync(tmp_path, date(2026, 10, 4))["records"] == 1
    pointer = tmp_path / "bond-transparency/CURRENT"
    previous = pointer.read_bytes()
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("data.xml", "<ChangedSchema/>")
    with pytest.raises(ValueError, match="no recognised"):
        module.sync(tmp_path, date(2026, 10, 5))
    assert pointer.read_bytes() == previous


def test_non_equity_paths_keep_pre_post_thresholds_and_missing_dates_separate(tmp_path):
    path = tmp_path / "test.zip"
    xml = f"""<Document xmlns="urn:iso:std:iso:20022:tech:xsd:auth.045.001.03">
    <NonEqtyTrnsprncyData><TechRcrdId>1</TechRcrdId>
    <Id><ISINAndSubClss><ISIN>{ISIN}</ISIN><FinInstrmClssfctn>BOND</FinInstrmClssfctn>
    <DerivSubClss><Desc>Corporate bond</Desc></DerivSubClss></ISINAndSubClss></Id>
    <RptgPrd><FrDtToDt><FrDt>2025-01-01</FrDt><ToDt>2025-12-31</ToDt></FrDtToDt></RptgPrd>
    <PreTradLrgInScaleThrshld><Amt Ccy="EUR">100000.00</Amt></PreTradLrgInScaleThrshld>
    <PstTradLrgInScaleThrshld><Amt Ccy="EUR">200000</Amt></PstTradLrgInScaleThrshld>
    <PreTradInstrmSzSpcfcThrshld><Amt Ccy="EUR">50000</Amt></PreTradInstrmSzSpcfcThrshld>
    </NonEqtyTrnsprncyData>
    <NonEqtyTrnsprncyData><Id><DerivSubClss><Desc>Subclass only</Desc></DerivSubClss></Id>
    </NonEqtyTrnsprncyData></Document>"""
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("test.xml", xml)
    rows = list(
        records(
            path,
            {
                "file_name": "test.zip",
                "download_link": "https://example.test",
                "creation_date": "2026-10-03",
            },
            "synthetic",
        )
    )
    assert rows[0]["lis_pre"] == "100000.00"
    assert rows[0]["lis_post"] == "200000"
    assert rows[0]["ssti_pre_currency"] == "EUR"
    assert rows[0]["application_from"] is None
    assert rows[1]["isin"] is None
    directory = tmp_path / "bond-transparency" / ("a" * 64)
    directory.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), directory / "results.parquet")
    (directory / "meta.json").write_text(json.dumps({"snapshot_date": "2026-10-03"}))
    (directory.parent / "CURRENT").write_text(directory.name)
    result = instrument(tmp_path, ISIN, date(2026, 10, 4))
    assert result["state"] == "APPLICABILITY_NOT_VERIFIED"
    assert result["applicable_record"] is None
    assert len(result["records"]) == 1
    assert "waiver" in result["notice"]
    assert instrument(tmp_path, "ES0178430E18", date(2026, 10, 4))["records"] == []
