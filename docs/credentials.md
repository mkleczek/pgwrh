# Replica credentials and rotation

Replicas need credentials to read shards from one another. Sharing one password
across the group means a single compromised replica exposes the login used by
the entire group. Credentials also need to be replaced periodically or when a
leak is suspected, with updates coordinated so reads keep working.

pgwrh gives each replica its own login and random password. Other replicas
receive the information needed to verify that login, without receiving its
reusable password. You can rotate the group's credentials with one command;
pgwrh installs new credentials, switches reads to them, and retires the old
logins after replicas confirm the switch.

This covers replica-to-replica reads. You still manage controller logins,
logical replication credentials and application logins separately.

## Authentication setup

On every replica, allow connections from your replica network using
`scram-sha-256` in `pg_hba.conf`. The following rule matches pgwrh's generated
login names. Replace the example network with your actual replica network and
place the rule before broader rules:

```text
host all /^pgwrh_[0-9a-f]{32}$ 192.0.2.0/24 scram-sha-256
```

Use `hostssl` where TLS is required and configured. pgwrh manages database
credentials; you remain responsible for network access, TLS and certificate
verification, including connections to the controller that distribute passwords.

## Rotate credentials

Run on the controller as its administrator, replacing `readers` with your
replication group name:

```sql
SELECT pgwrh.rotate_credentials('readers');
```

Rotation replaces credentials for the whole group, with one rotation allowed
at a time. Replicas install the replacements and switch over automatically.
Check progress with:

```sql
SELECT * FROM pgwrh.credential_rotation
WHERE replication_group_id = 'readers';
```

Wait until only the new `active` row remains. This means every replica has
confirmed the switch; old login permissions are removed on subsequent sync
passes. Offline replicas, stopped sync workers and long-running reads can delay
rotation. Existing credentials remain usable until the required confirmations
arrive. See [rotation diagnostics](#rotation-diagnostics) if progress stops.

Use your existing job scheduler to run periodic rotations, allowing each one to
finish before starting another. pgwrh does not impose a timer. Adding replicas
or starting, committing or rolling back a placement rollout does not rotate
existing credentials, and a rollback cannot restore old passwords.

## Compromise and recovery

A replica holds its own password for reading other replicas, not their reusable
passwords. If it is compromised, an attacker can use that identity to read data
across the group. Its separately managed controller login is also at risk.

Isolate an affected replica and address its controller login and existing
sessions as part of recovery. Routine rotation preserves ongoing reads and
does not terminate open sessions, so it does not provide an immediate lockout.

Controller backups contain replica passwords and credential state. Protect
them as secrets and follow the [recovery procedure](recovery.md) when restoring
a controller.

## Technical details

### Password storage and access

The controller generates the passwords; replicas do not generate them. Each
replica uses its own identity when reading any other replica in its group.
For incoming connections, replicas receive SCRAM-SHA-256 verifiers rather than
reusable passwords, with an independent salt for each PostgreSQL server.

Generated foreign servers set `require_auth=scram-sha-256`. Their connections
require SCRAM even if a broader authentication rule would otherwise allow
`trust` or a cleartext password challenge. The controller uses PostgreSQL's
native SCRAM implementation and its `scram_iterations` setting. Changing that
setting affects the next credential generation, not existing credentials. See
PostgreSQL's [SCRAM iteration policy](https://www.postgresql.org/docs/18/runtime-config-connection.html#GUC-SCRAM-ITERATIONS).

The replica sync plan is administrator-only because its commands can contain
outbound passwords. Incoming replica logins receive the shared local reader
role, without administrative access to credentials or sync commands. The
controller login supplied to `configure_controller` is administered separately.

### Rotation diagnostics

Each rotation returns a UUID identifying a new credential generation. While
`preparing`, `missing_installations` identifies unreported incoming logins.
Inspect the missing installations with:

```sql
SELECT * FROM pgwrh.missing_credential_installation
WHERE replication_group_id = 'readers';
```

After activation, `unconfirmed_sources` on the **active** row counts replicas
that have not finished switching. Find them with:

```sql
SELECT m.member_role
FROM pgwrh.replication_group_member m
JOIN pgwrh.credential_generation g USING (replication_group_id)
WHERE g.replication_group_id = 'readers' AND g.state = 'active'
  AND m.credential_generation IS DISTINCT FROM g.generation;
```

A host's `online=false` flag excludes it from routing; it does not remove its
obligation to install credentials and acknowledge its outbound generation.
After a controller restore, the recovery procedure clears saved
acknowledgements before replicas reconnect, so rotation depends on fresh reports.

### Multiple databases on one server

When several replica databases share one PostgreSQL server, register them with
the same canonical host name and port. PostgreSQL roles are server-wide, so
these databases share that server's verifier for each incoming login. Each
database maintains its own role grant. Different aliases for the same server
cannot be recognized automatically and must not be used as separate verifier
domains.

### Limits of rotation tracking

Acknowledgements cover managed query roots and their prepared replacements.
Detachment is not an access-control boundary: sufficiently privileged roles can
query retained foreign tables directly. Actual servers have PUBLIC user mappings
but are created without PUBLIC USAGE; applications read managed tables through
routes authorized by the virtual server owner. Roles explicitly granted server
USAGE and schema CREATE can create their own foreign tables. Such direct uses
are outside the managed-route drain guarantee. Old incoming logins retire
regardless of unused detached mappings; existing sessions are not forcibly
disconnected by this rotation protocol.
