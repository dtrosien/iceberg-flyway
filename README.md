# iceberg-flyway

Proof of concept: versioned migrations for Apache Iceberg tables with Flyway (Apache 2.0
community edition only) and the Iceberg Java API. This branch has no effect system: the code
is plain synchronous Scala (cats-core and decline only); the `cats-effect` branch has the same code on
cats-effect IO.

## How it works

Flyway needs a relational database for its schema history table and its lock; Iceberg cannot
provide that. So Flyway talks to PostgreSQL only for bookkeeping, and every migration is a
`BaseJavaMigration` that ignores the JDBC connection and changes Iceberg tables through a
`Catalog` instead:

- `migration/IcebergMigration` – base class. Subclasses follow Flyway's naming scheme
  (`V2__Add_user_email` is version 2, "Add user email"), receive the `Catalog` in their
  constructor and implement `run()`. Helpers, each one Iceberg transaction that ends with the
  schema stamp below:
  - `createTable(id, schema, properties, partitioning)` – creates the namespace if needed and
    the table with its initial table properties and partition spec. `partitioning` gets a
    `PartitionSpec.Builder` for the schema, e.g. `_.identity("country").day("created_at")`;
    the default is unpartitioned.
  - `evolvePartitioning(id, partitioning)` – declares the target spec the same way and diffs it
    against the current one: fields are matched by transform and source column, missing ones are
    removed, new ones added at the end, and a field that only differs in name is renamed.
    Existing data files keep their old layout, as Iceberg partition evolution always does.
  - `evolveTable(id, schema)` – `UpdateSchema.unionByNameWith`: adds missing columns as
    optional, relaxes required columns, then reorders the columns to the case class order;
    never drops, renames or narrows.
  - `updateProperties(id, set, remove)` – sets and removes table properties (`V4` tunes file
    size and snapshot retention of `users`).
  - `adoptTable(id, expectedSchema)` – takes over a table that was created manually, see below.
- `migration/SchemaStamp` – every migration records the column layout, the partition fields and
  the table properties it produced in the table property `iceberg-flyway.schema`. `migrate` checks all tables in
  `model.Tables.all` against their stamp before Flyway starts, and every helper checks again
  before changing a table; a mismatch means the table was changed outside of migrations and the
  run fails with `TableDriftException`. A table without the stamp is not managed yet: the
  pre-flight check skips it, but `evolveTable` and `updateProperties` refuse to touch it.
- `migrations/Migrations.all` – the explicit list of migration instances. Flyway gets them via
  `javaMigrations(...)`; classpath scanning is switched off, so nothing runs by accident.
- `migration/IcebergFlyway` – configures Flyway (`dataSource`, a schema of its own for the
  history table) and exposes `migrate`, `validate` and `info`. Failures propagate as
  exceptions, so `MigrateApp` exits non-zero.
- `iceberg/IcebergSchemas.schemaFor[T]` – case class to Iceberg `Schema`:
  [flink-rowdata4s](https://github.com/dtrosien/flink-rowdata4s) derives the Flink `DataType`,
  `FlinkSchemaUtil` from iceberg-flink converts it. `Option[A]` becomes an optional column,
  everything else is required; `@TableName` renames, `@TableComment` becomes the column doc,
  `@TableTransient` drops a field. Field ids are reassigned by Iceberg on create and matched by
  name on evolution.
- `iceberg/IcebergData.append` – writes `Record`s into one Parquet file per partition and
  commits them together, for seeding reference data.
- `iceberg/IcebergCatalogs.resource` – builds the catalog from config via
  `CatalogUtil.buildIcebergCatalog`.

The example migrations create `app.users` from a frozen snapshot of the model, partitioned by
`day(created_at)` (`V1`), bring it
in line with the current `model.User` case class (`V2`, adds the optional `email`), create
and seed `app.countries` from a snapshot (`V3`) and set table properties on `users` (`V4`).
Migrations that create tables use snapshots
(`UserV1`, `CountryV3`) because Java migrations have no checksum: if they used the live model,
changing the model would silently change what the migration does on a fresh environment.
`MigrationTest` asserts that after all migrations the table matches the model the apps use,
column order included, which is the check to keep in a real repository.

### Changing a model

Add the field to the case class and write a migration that calls
`evolveTable(Tables.x, IcebergSchemas.schemaFor[X])`. Rules that follow from
`unionByNameWith`:

- New fields must be `Option[...]`. Existing rows have no value, so Iceberg adds them as
  optional; a required field in the model would fail the model check.
- Position in the case class is honoured: the column is moved there (metadata only).
- Widening (int to long, float to double, higher decimal precision) is applied; narrowing is
  silently ignored; any other type change fails the migration before it commits.
- A rename is not detected: it adds a new column and keeps the old one. Call `renameColumn`
  explicitly in the migration instead.

### Taking over a table that was created manually

Tables that already exist (created by Spark, Athena, Terraform, ...) are brought under
management by a migration that calls `adoptTable(Tables.x, IcebergSchemas.schemaFor[XAsFound])`
with a snapshot of the columns you expect to find. The columns must match by name, type and
optionality; order does not matter and is fixed by a following `evolveTable`. If the table looks
different the migration fails and nothing is stamped, so adoption is reviewed code, not a guess.
Use `if catalog.tableExists(id) then adoptTable(...) else createTable(...)` when fresh
environments do not have the table. The `stamp` command also stamps an unmanaged table, but
without any check; prefer `adoptTable`.

### A table was changed outside of migrations

`migrate` stops with `TableDriftException` naming the table, the recorded and the actual
columns and properties. Either revert the manual change, or keep it:

1. write a migration that reproduces the change, so every other environment gets it too;
2. in the drifted environment run `stamp app.countries` to accept the current layout;
3. run `migrate`; the new migration is a no-op there and applies everywhere else.

`stamp` is the Iceberg counterpart of `flyway repair`: it trusts the operator and should be a
deliberate step in CI, not part of the regular deployment.

Things to know:

- Migrations run with Flyway's default `canExecuteInTransaction = true`. The Postgres
  transaction cannot roll back Iceberg commits; a failing migration leaves no history row and
  is retried on the next run. The helpers are safe to re-run (`createTable` accepts its own
  earlier result, `evolveTable` and `updateProperties` are idempotent); anything else a
  migration does, such as seeding rows, must guard itself (`V3` seeds only when the table has
  no snapshot).
- Writing `Record`s by hand is fine for seeds. Encoding case classes to Iceberg rows via
  rowdata4s (`RowData`) is not covered here.
- Hadoop stays on the classpath because Iceberg's Parquet writer needs
  `org.apache.hadoop.conf.Configuration`, and the local default catalog is a Hadoop catalog.
- Migrations are plain Scala with decline for the CLI; there is no Flink runtime or Kafka in
  this project, the apps bring their own.

## Copying this into a project

The repositories this is meant for hold Flink apps next to their migrations, with case classes
shared between both. Suggested layout: an sbt subproject `migrations` with its own main class and
Docker image, depending on the subproject that holds the shared case classes.

Copy as is (generic, no project knowledge inside):

- `migration/` – `IcebergMigration`, `SchemaStamp`, `IcebergFlyway`, `MigrateApp` (the generic
  CLI main)
- `iceberg/` – `IcebergSchemas`, `IcebergData`, `IcebergCatalogs`
- `MigrationConfig.scala`, the `migration` section of `application.conf`, `logback.xml`
- from `build.sbt`: the dependency block (Flyway, Iceberg, Hadoop, aws, rowdata4s plus
  `flink-table-api-java`), `Compile / mainClass`, the fork and Testcontainers settings and the
  Docker settings. Keep `iceberg-flink-<version>` and `flink-table-api-java` on the Flink
  version the apps use.
- tests: `UnitSpec` and `IcebergCatalogsTest` as they are; `MigrationTest` as the template for
  the project's own end-to-end test (swap in its tables and models). `IcebergMigrationTest`
  tests the helpers themselves and only needs to travel with them.

Own per project:

- `model/` – the case classes the apps and migrations share, and `Tables` with `Tables.all`
  (the list the pre-flight drift check runs over)
- `migrations/` – the `V<n>__...` classes and `Migrations.all`
- the main object, one line: `object MigrateMain extends MigrateApp(Migrations.all, Tables.all)`
- `flyway.schema` (one per migration set when several share a database) and the Docker image name

If more than one repository needs the generic part, publish `migration/` and `iceberg/` as a
small library instead of copying; this build already has sbt-ci-release wired up. Projects then
depend on it and only write models and migrations.

### Multi-project repositories

For a build with several apps and shared model modules, keep one migration set per app (or per
model, see below) as its own subproject:

```
root
├── models/model1, models/model2
├── apps/app1, apps/app2 (→ model1), apps/app3 (→ model2)      # built with sbt-assembly
├── iceberg-migrations                                          # the generic part of this repo
└── migrations/app1, migrations/app2 (→ iceberg-migrations, model1)
    migrations/app3 (→ iceberg-migrations, model2)
```

- Each `migrations/<app>` holds its `V<n>__...` classes, `Migrations.all`, `Tables.all`, an
  `application.conf` with its own `flyway.schema` (one Flyway schema per set, all in the same
  Postgres database) and `object MigrateMain extends MigrateApp(Migrations.all, Tables.all,
  name = "app1-migrations")`.
- Migration subprojects depend on the model modules, never on the app modules, so no Flink
  runtime or Kafka ends up on their classpath.
- **One owner per table.** app1 and app2 share model1; if they also share tables, exactly one
  migration set must own each table, otherwise two Flyway histories evolve it and the stamp
  reports each other's commits as drift. Either assign shared tables to one app, or give shared
  models a migration set of their own. `Tables.all` makes the ownership explicit.
- Pipeline per app: build, run that app's migration job (`java -jar app1-migrations.jar migrate`
  or the Docker image), then deploy the app. Sets do not block each other; Flyway's lock is per
  history table.
- Building the migration jars with sbt-assembly needs three merge rules, otherwise the jar starts
  but misbehaves: concatenate `META-INF/services/*` (`MergeStrategy.filterDistinctLines`; both
  flyway-core and flyway-database-postgresql ship
  `META-INF/services/org.flywaydb.core.extensibility.Plugin`, and Iceberg, Jackson 3 and the AWS
  bundle also use service files), discard `module-info.class` and `META-INF/*.SF|*.DSA|*.RSA`, and
  take `first` for Hadoop's duplicated resource files such as `core-default.xml`. Using
  sbt-native-packager for the migration subprojects only, as this repo does, avoids the merge
  rules.

### Why a main class instead of the Flyway CLI

Flyway does ship a command line tool (also as the `flyway/flyway` Docker image) and it can run
Java migrations from jars dropped into its `jars/` folder. It does not fit here: the CLI
instantiates Java migrations by classpath scanning through a no-arg constructor, so the Iceberg
`Catalog` could not be injected; the whole Iceberg, Hadoop and AWS classpath would have to be
copied into the CLI's folder; and the pre-flight drift check, the `stamp` repair and the
pureconfig configuration would have no place to run. Embedding Flyway through its Java API in a
small main is the usual approach when migrations are code with dependencies of their own
(Spring Boot does the same at startup); the CLI shines for plain SQL migrations.

## Configuration

`src/main/resources/application.conf`, section `migration`, loaded by `MigrationConfig`:

| Key | Env override | Default |
|---|---|---|
| `flyway.url` | `FLYWAY_URL` | `jdbc:postgresql://localhost:5432/postgres` |
| `flyway.user` | `FLYWAY_USER` | `postgres` |
| `flyway.password` | `FLYWAY_PASSWORD` | `postgres` |
| `flyway.schema` | `FLYWAY_SCHEMA` | `iceberg_flyway` |
| `iceberg.properties.type` | `ICEBERG_CATALOG_TYPE` | `hadoop` |
| `iceberg.properties.warehouse` | `ICEBERG_WAREHOUSE` | `file://<project>/target/iceberg-warehouse` |
| `iceberg.properties.io-impl` | `ICEBERG_IO_IMPL` | unset |

`iceberg.properties` is passed verbatim to Iceberg, so any catalog property can be added.
Migrations also create the Iceberg namespace (`app`) if it is missing. On AWS that is a Glue
database; if infrastructure code owns databases, drop `ensureNamespace` and manage it there.
`type` is always set, so a catalog that needs `catalog-impl` instead takes a replacement
config file (`-Dconfig.file`), not an added key.

Flyway creates `flyway.schema` and keeps `flyway_schema_history` in it. Several migration sets
can share one database by using different schemas; sharing a schema does not work, because
Flyway refuses to start in a non-empty schema that has no history table.

## Run locally

```
docker run -d --name pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:alpine
sbt --server "run migrate"                 # apply pending migrations, print the result, exit
sbt --server "run stamp app.users"         # accept a table's current layout (see above)
```

## Running on AWS

The target setup is an ECS Fargate one-off task started by CI after the image is pushed:

- Flyway history: RDS or Aurora PostgreSQL. The migration needs network access to it and the
  JDBC URL, user and password injected by CI as `FLYWAY_*` environment variables
  (Flyway's own Secrets Manager integration is a paid feature and is not used). The database
  user must be allowed to create the `flyway.schema` schema.
- Iceberg: Glue catalog with S3 storage. Set `ICEBERG_CATALOG_TYPE=glue` and
  `ICEBERG_WAREHOUSE=s3://bucket/path`; the Glue catalog uses `S3FileIO` by default, so
  `ICEBERG_IO_IMPL` is only needed for another FileIO. `iceberg-aws` plus `iceberg-aws-bundle`
  (the shaded AWS SDK) are on the classpath. The task role needs Glue database/table
  permissions and read/write on the warehouse bucket; credentials and region come from the
  task's environment through the SDK's default provider chain. Nothing in this repo talks to
  AWS, so the Glue path is only verified by loading `S3FileIO` in `IcebergCatalogsTest`.
- Packaging: sbt-native-packager builds a Docker image on `eclipse-temurin:21-jre`:

  ```
  sbt Docker/publishLocal                       # image iceberg-flyway:<version>
  docker run --rm -e FLYWAY_URL=... -e FLYWAY_USER=... -e FLYWAY_PASSWORD=... \
    -e ICEBERG_CATALOG_TYPE=glue -e ICEBERG_WAREHOUSE=s3://bucket/path \
    iceberg-flyway:<version> migrate
  ```

  The launcher passes its arguments to `MigrateApp`, so the ECS task command is `migrate`
  (or `stamp app.users` for repairs). Logging goes through logback (`logback.xml`: Flyway at
  INFO, everything else at WARN) so Iceberg and AWS SDK warnings reach CloudWatch. The image keeps the whole dependency classpath,
  including `tools.jackson` (Jackson 3, Flyway 12 fails without it) and `iceberg-orc` (needed
  at runtime for reads).
- Concurrent runs are safe: Flyway takes a PostgreSQL advisory lock around each migration.

## Tests

`MigrationTest` starts `postgres:alpine` with Testcontainers (Ryuk is disabled in
`build.sbt`, the suite stops its container itself) and uses Iceberg's `InMemoryCatalog`.
`IcebergMigrationTest` covers the table helpers without Flyway: stamps, column order,
idempotency and the drift failure plus `stamp` repair.
`IcebergCatalogsTest` builds a Hadoop catalog on an os-lib temp directory and removes it
afterwards. Tests run in a forked JVM so the Testcontainers setting in `build.sbt` applies.

## Test and format

```
sbt testFull                                  # all tests (`sbt test` is incremental in sbt 2)
sbt scalafmtAll                               # format sources
sbt "coverage; testFull; coverageReport"      # tests with a coverage report
sbt --server --batch "scalafmtSbtCheck; scalafmtCheckAll; coverage; testFull; coverageReport"   # what CI runs
```

Tests use [ScalaTest](https://www.scalatest.org/) in the flink-rowdata4s style (`UnitSpec`).
Formatting is defined in
`.scalafmt.conf`, and `.github/workflows/ci.yml` runs the same checks on every push and pull
request.

## Release

The version comes from git tags via sbt-ci-release: pushing `vX.Y.Z` publishes a
release, pushes to `no-cats-effect` (the default branch) publish snapshots. `.github/workflows/release.yml` needs the
`PGP_PASSPHRASE`, `PGP_SECRET`, `SONATYPE_USERNAME` and `SONATYPE_PASSWORD` secrets;
see the [sbt-ci-release](https://github.com/sbt/sbt-ci-release) README.

## Working with coding agents

- `AGENTS.md` holds the project instructions; `CLAUDE.md` imports it for Claude Code.
- `.claude/skills/` documents ScalaTest and the `cellar` CLI.
- `.mcp.json` starts a Metals MCP server (`metals-mcp`, installed with `cs install metals-mcp`)
  when Claude Code opens the project; `.claude/settings.json` allows the build tools
  without prompts.
