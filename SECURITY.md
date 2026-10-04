# Security scope

This tool downloads public data only when explicitly invoked and reads snapshots offline.
Data downloads use approved HTTPS hosts and bounded payloads; XML entity expansion is
disabled where XML is parsed. Dependencies and CI actions are pinned. Source data is not
included and source licences remain separate from the MIT code licence.

CI checks tests, lint and known dependency vulnerabilities on changes and weekly.
Bandit reports are visible in the job log: scanner candidates require source review and
are not automatically classified as exploitable vulnerabilities. The static report is
informational; dependency vulnerabilities and tests fail the check.

Do not attach participant exports, credentials or licensed raw data to public issues.
Report suspected vulnerabilities privately through GitHub's security reporting feature
if enabled; otherwise contact the repository owner privately before public disclosure.
