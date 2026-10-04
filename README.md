# ESMA FITRS Reader

Capture public ESMA transparency files and query exact published results offline.

```bash
uv sync --locked --extra dev
uv run esma-fitrs-reader sync --scope equity
uv run esma-fitrs-reader instrument --isin ES0178430E18 > telefonica.csv
uv run esma-fitrs-reader sync --scope debt
uv run esma-fitrs-reader instrument --scope debt --isin XS3373438483 > instrument.csv
uv run pytest -q
```

Equity support: FULECR/DLTECR, explicit calculation/application dates, exact decimal text,
ESTM/FFWK/YEAR, overlap detection. A fresh file does not make an expired result current.
Debt support: FULNCR/DLTNCR debt-CFI files, streamed parsing, separate pre/post LIS and SSTI,
separate ISIN/subclass records. Missing application dates are never inferred. Debt results
are observations requiring applicability review, not execution permission. This is not
complete coverage of every derivative category or every historical revision.

Only sync accesses the network. Queries use captured Parquet. No paid feed, broker login,
OpenInstrument, OpenVenue server or dataset is required. No data is shipped in this repo.

Code: MIT. ESMA data and legal applicability retain their source conditions. Unofficial
project, not endorsed by ESMA. SSTI publication does not restore its former waiver.

Sources: [download instructions](https://www.esma.europa.eu/document/firds-transparency-download-instructions),
[non-equity guidance](https://www.esma.europa.eu/annual-transparency-calculations-non-equity-instruments).
