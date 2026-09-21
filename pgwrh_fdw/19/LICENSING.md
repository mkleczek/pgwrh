# Licensing

The pgwrh_fdw fork, including its original additions and modifications, is
licensed under the **GNU Affero General Public License, version 3 only**
(`AGPL-3.0-only`). See [LICENSE](LICENSE) for the full terms. The references to
“or any later version” in the license document's example application notice do
not grant a later-version option for this project.

Copyright (c) 2026, pgwrh_fdw contributors.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for details.

## PostgreSQL material

The fork contains PostgreSQL's `contrib/postgres_fdw` code, installation SQL,
tests and history. That upstream material retains its original copyrights and
PostgreSQL License permissions, reproduced verbatim in [COPYRIGHT](COPYRIGHT)
and preserved in the source files. This project does not revoke those permissions
or claim ownership of the original PostgreSQL code.

Changes made for pgwrh_fdw on or after 2026-09-14 are provided under AGPL-3.0-only.
This includes changes to inherited files and the new transaction-context code,
test harness, documentation, build additions and CI configuration. The original
upstream branch and release tag remain unmodified.

## Corresponding source

Current source and build/test instructions are maintained in the `pgwrh_fdw/`
subdirectory of https://github.com/mkleczek/pgwrh. The initial standalone release
came from https://github.com/mkleczek/pgwrh_fdw and its history is preserved.
A source offer for a deployed modified
version must cover the corresponding source for that version, including its
modifications and required build material, as specified by the license.
Publishing this repository does not automatically publish an operator's later
changes or satisfy every downstream source-offer obligation.

AGPL is a copyright license, not an ownership claim over database contents or
application data. Questions about combined works, other modules loaded into
PostgreSQL, or a particular deployment should be reviewed in that context.
