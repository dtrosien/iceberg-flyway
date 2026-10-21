package com.example.icebergflyway.migration

import com.example.icebergflyway.FlywayConfig
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.MigrationInfoService
import org.flywaydb.core.api.migration.JavaMigration
import org.flywaydb.core.api.output.MigrateResult

/** Flyway configured with explicit migration instances (no classpath scanning). */
final class IcebergFlyway private (flyway: Flyway):
  def migrate(): MigrateResult     = flyway.migrate()
  def validate(): Unit             = flyway.validate()
  def info(): MigrationInfoService = flyway.info()

object IcebergFlyway:
  def apply(config: FlywayConfig, migrations: Seq[JavaMigration]): IcebergFlyway =
    new IcebergFlyway(
      Flyway
        .configure()
        .dataSource(config.url, config.user, config.password)
        .locations(Seq.empty[String]*) // no scanning: migrations are the instances below
        .javaMigrations(migrations*)
        .defaultSchema(config.schema)
        .load()
    )
