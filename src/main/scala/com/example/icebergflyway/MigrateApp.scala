package com.example.icebergflyway

import cats.data.NonEmptyList
import com.example.icebergflyway.iceberg.IcebergCatalogs
import com.example.icebergflyway.migration.{IcebergFlyway, SchemaStamp}
import com.example.icebergflyway.migrations.Migrations
import com.example.icebergflyway.model.Tables
import com.monovore.decline.{CommandApp, Opts}
import org.apache.iceberg.catalog.{Catalog, TableIdentifier}

/** `migrate` applies pending migrations and exits; `stamp` is the repair path after a table was
  * changed outside of migrations. Failures propagate as exceptions, so the process exits non-zero.
  */
object MigrateApp
    extends CommandApp(
      name = "iceberg-flyway",
      header = "Flyway migrations for Iceberg tables",
      main = Commands.all
    )

private object Commands:
  private val migrateCommand = Opts.subcommand(
    "migrate",
    "Apply pending migrations; fails if a managed table was changed outside of migrations"
  )(Opts.unit)

  private val stampCommand = Opts.subcommand(
    "stamp",
    "Record the current schema of the given tables (e.g. app.users) as the migrated state"
  )(Opts.arguments[String]("table"))

  val all: Opts[Unit] =
    migrateCommand.map(_ => withCatalog(migrate)) orElse
      stampCommand.map(tables => withCatalog((_, catalog) => stamp(tables, catalog)))

  private def withCatalog(run: (MigrationConfig, Catalog) => Unit): Unit =
    val config = MigrationConfig.load()
    IcebergCatalogs.withCatalog(config.iceberg)(run(config, _))

  private def migrate(config: MigrationConfig, catalog: Catalog): Unit =
    SchemaStamp.verifyAll(catalog, Tables.all)
    val result = IcebergFlyway(config.flyway, Migrations.all(catalog)).migrate()
    // targetSchemaVersion is null when nothing was pending; the initial version is current then
    val version = Option(result.targetSchemaVersion).orElse(Option(result.initialSchemaVersion))
    println(
      s"Applied ${result.migrationsExecuted} migration(s), schema version ${version.getOrElse("none")}"
    )

  private def stamp(tables: NonEmptyList[String], catalog: Catalog): Unit =
    val ids = tables.toList.map(TableIdentifier.parse)
    SchemaStamp.stampAll(catalog, ids)
    println(s"Stamped ${ids.mkString(", ")}")
