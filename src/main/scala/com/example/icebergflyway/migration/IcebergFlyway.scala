package com.example.icebergflyway.migration

import cats.effect.IO
import com.example.icebergflyway.FlywayConfig
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.MigrationInfoService
import org.flywaydb.core.api.migration.JavaMigration
import org.flywaydb.core.api.output.MigrateResult

/** Flyway configured with explicit migration instances (no classpath scanning), run inside IO. */
final class IcebergFlyway private (flyway: Flyway):
  def migrate: IO[MigrateResult]     = IO.blocking(flyway.migrate())
  def validate: IO[Unit]             = IO.blocking(flyway.validate())
  def info: IO[MigrationInfoService] = IO.blocking(flyway.info())

object IcebergFlyway:
  /** Configuring Flyway loads the JDBC driver and scans plugins, hence IO. */
  def apply(config: FlywayConfig, migrations: Seq[JavaMigration]): IO[IcebergFlyway] =
    IO.blocking(
      new IcebergFlyway(
        Flyway
          .configure()
          .dataSource(config.url, config.user, config.password)
          .locations(Seq.empty[String]*) // no scanning: migrations are the instances below
          .javaMigrations(migrations*)
          .defaultSchema(config.schema)
          .load()
      )
    )
