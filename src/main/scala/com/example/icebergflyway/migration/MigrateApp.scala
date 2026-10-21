package com.example.icebergflyway.migration

import cats.data.NonEmptyList
import com.example.icebergflyway.MigrationConfig
import com.example.icebergflyway.iceberg.IcebergCatalogs
import com.monovore.decline.{CommandApp, Opts}
import org.apache.iceberg.catalog.{Catalog, TableIdentifier}
import org.flywaydb.core.api.migration.JavaMigration

/** Main class for one migration set: `migrate` applies pending migrations and exits, `stamp` is the
  * repair path after a table was changed outside of migrations. A project declares
  * `object Main extends MigrateApp(Migrations.all, Tables.all)`. Failures propagate as exceptions,
  * so the process exits non-zero.
  */
abstract class MigrateApp(
    migrations: Catalog => List[JavaMigration],
    tables: List[TableIdentifier],
    name: String = "iceberg-flyway"
) extends CommandApp(
      name = name,
      header = "Flyway migrations for Iceberg tables",
      main = MigrateApp.commands(migrations, tables)
    )

object MigrateApp:
  private val migrateCommand = Opts.subcommand(
    "migrate",
    "Apply pending migrations; fails if a managed table was changed outside of migrations"
  )(Opts.unit)

  private val stampCommand = Opts.subcommand(
    "stamp",
    "Record the current schema of the given tables (e.g. app.users) as the migrated state"
  )(Opts.arguments[String]("table"))

  private def commands(
      migrations: Catalog => List[JavaMigration],
      tables: List[TableIdentifier]
  ): Opts[Unit] =
    migrateCommand.map(_ => withCatalog(migrate(migrations, tables))) orElse
      stampCommand.map(ids => withCatalog((_, catalog) => stamp(ids, catalog)))

  private def withCatalog(run: (MigrationConfig, Catalog) => Unit): Unit =
    val config = MigrationConfig.load()
    IcebergCatalogs.withCatalog(config.iceberg)(run(config, _))

  private def migrate(migrations: Catalog => List[JavaMigration], tables: List[TableIdentifier])(
      config: MigrationConfig,
      catalog: Catalog
  ): Unit =
    SchemaStamp.verifyAll(catalog, tables)
    val result = IcebergFlyway(config.flyway, migrations(catalog)).migrate()
    // targetSchemaVersion is null when nothing was pending; the initial version is current then
    val version = Option(result.targetSchemaVersion).orElse(Option(result.initialSchemaVersion))
    println(
      s"Applied ${result.migrationsExecuted} migration(s), schema version ${version.getOrElse("none")}"
    )

  private def stamp(tables: NonEmptyList[String], catalog: Catalog): Unit =
    val ids = tables.toList.map(TableIdentifier.parse)
    SchemaStamp.stampAll(catalog, ids)
    println(s"Stamped ${ids.mkString(", ")}")
