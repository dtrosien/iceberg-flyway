package com.example.icebergflyway.migration

import cats.data.NonEmptyList
import cats.effect.*
import cats.syntax.all.*
import com.example.icebergflyway.MigrationConfig
import com.example.icebergflyway.iceberg.IcebergCatalogs
import com.monovore.decline.Opts
import com.monovore.decline.effect.CommandIOApp
import org.apache.iceberg.catalog.{Catalog, TableIdentifier}
import org.flywaydb.core.api.migration.JavaMigration

/** Main class for one migration set: `migrate` applies pending migrations and exits, `stamp` is the
  * repair path after a table was changed outside of migrations. A project declares
  * `object Main extends MigrateApp(Migrations.all, Tables.all)`.
  */
abstract class MigrateApp(
    migrations: Catalog => List[JavaMigration],
    tables: List[TableIdentifier],
    name: String = "iceberg-flyway"
) extends CommandIOApp(name = name, header = "Flyway migrations for Iceberg tables"):

  private val migrateCommand = Opts.subcommand(
    "migrate",
    "Apply pending migrations; fails if a managed table was changed outside of migrations"
  )(Opts.unit)

  private val stampCommand = Opts.subcommand(
    "stamp",
    "Record the current schema of the given tables (e.g. app.users) as the migrated state"
  )(Opts.arguments[String]("table"))

  def main: Opts[IO[ExitCode]] = migrateCommand.as(migrate) orElse stampCommand.map(stamp)

  private def withCatalog(run: (MigrationConfig, Catalog) => IO[Unit]): IO[ExitCode] =
    MigrationConfig.load
      .flatMap(config => IcebergCatalogs.resource(config.iceberg).use(run(config, _)))
      .as(ExitCode.Success)

  private val migrate: IO[ExitCode] = withCatalog { (config, catalog) =>
    for
      _      <- SchemaStamp.verifyAll(catalog, tables)
      flyway <- IcebergFlyway(config.flyway, migrations(catalog))
      result <- flyway.migrate
      // targetSchemaVersion is null when nothing was pending; the initial version is current then
      version = Option(result.targetSchemaVersion).orElse(Option(result.initialSchemaVersion))
      _ <- IO.println(
        s"Applied ${result.migrationsExecuted} migration(s), schema version ${version.getOrElse("none")}"
      )
    yield ()
  }

  private def stamp(ids: NonEmptyList[String]): IO[ExitCode] = withCatalog { (_, catalog) =>
    val parsed = ids.toList.map(TableIdentifier.parse)
    SchemaStamp.stampAll(catalog, parsed) *> IO.println(s"Stamped ${parsed.mkString(", ")}")
  }
