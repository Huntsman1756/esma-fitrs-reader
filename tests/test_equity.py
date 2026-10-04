import json
import zipfile
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq

from esma_fitrs.equity import Transparency, parse

ISIN = "ES0178430E18"


def test_equity_xml_preserves_decimals_and_explicit_application_period(tmp_path):
    xml = f"""<Document xmlns="urn:iso:std:iso:20022:tech:xsd:auth.044.001.03">
    <EqtyTrnsprncyData><TechRcrdId>1</TechRcrdId><Id>{ISIN}</Id>
    <Mthdlgy>YEAR</Mthdlgy><Lqdty>true</Lqdty><FinInstrmClssfctn>SHRS</FinInstrmClssfctn>
    <RptgPrd><FrDtToDt><FrDt>2025-01-01</FrDt><ToDt>2025-12-31</ToDt></FrDtToDt></RptgPrd>
    <ApplPrd><FrDtToDt><FrDt>2026-04-06</FrDt><ToDt>2027-04-04</ToDt></FrDtToDt></ApplPrd>
    <Sttstcs><AvrgDalyTrnvr Ccy="EUR">89.5000</AvrgDalyTrnvr>
    <LrgInScale>500000</LrgInScale></Sttstcs>
    <RlvntMkt><Id>XMAD</Id><AvrgDalyNbOfTxs>4186.73563</AvrgDalyNbOfTxs></RlvntMkt>
    </EqtyTrnsprncyData></Document>"""
    path = tmp_path / "source.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("result.xml", xml)
    row = parse(
        path,
        {
            "creation_date": "2026-10-03",
            "file_name": "source.zip",
            "download_link": "https://example.test",
        },
        "sha",
    )[0]
    assert row["adt"] == "89.5000" and row["adt_currency"] == "EUR"
    assert row["application_from"] == "2026-04-06"
    assert row["reference_mic"] == "XMAD"
    assert row["sms"] is None  # Missing does not mean zero.
    assert row["locator"] == "result.xml#TechRcrdId=1"


def publish(tmp_path, rows):
    target = tmp_path / "transparency" / ("a" * 64)
    target.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(rows), target / "equity.parquet")
    (target / "meta.json").write_text(json.dumps({"snapshot_date": "2026-10-03"}))
    (target.parent / "CURRENT").write_text(target.name)
    return Transparency(tmp_path)


def result(begin, end, **extra):
    return {
        "isin": ISIN,
        "application_from": begin,
        "application_to": end,
        "report_to": "2025-12-31",
        "methodology": "YEAR",
        "lis": "500000",
        **extra,
    }


def test_fresh_file_does_not_make_expired_calculation_current(tmp_path):
    store = publish(
        tmp_path, [result("2018-04-01", "2019-03-31"), result("2027-04-05", "2028-04-04")]
    )
    data = store.instrument(ISIN, date(2026, 10, 4))
    assert data["applicable_record"] is None
    assert data["state"] == "NO_APPLICABLE_RESULT"
    assert {r["application_state"] for r in data["records"]} == {"EXPIRED", "FUTURE"}


def test_overlapping_results_are_not_arbitrarily_adjudicated(tmp_path):
    store = publish(
        tmp_path,
        [result("2026-04-06", "2027-04-04"), result("2026-04-06", "2027-04-04", lis="650000")],
    )
    data = store.instrument(ISIN, date(2026, 10, 4))
    assert data["state"] == "CONFLICT" and data["applicable_record"] is None


def test_unknown_application_period_and_si_are_not_used_as_current_regime(tmp_path):
    store = publish(
        tmp_path, [result(None, None), result("2026-04-06", "2027-04-04", methodology="SINT")]
    )
    assert store.instrument(ISIN, date(2026, 10, 4))["applicable_record"] is None
    assert store.instrument("XS3373438483", date(2026, 10, 4))["state"] == "NOT_IN_SNAPSHOT"


def test_unique_active_result_is_selected_without_mixing_periods(tmp_path):
    store = publish(
        tmp_path,
        [result("2018-04-01", "2019-03-31", lis="650000"), result("2026-04-06", "2027-04-04")],
    )
    assert store.instrument(ISIN, date(2026, 10, 4))["applicable_record"]["lis"] == "500000"
