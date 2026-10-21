package com.example.icebergflyway

import pureconfig.*

/** JDBC connection for Flyway's schema history. Flyway creates and owns `schema`; one schema per
  * migration set lets several sets share one database.
  */
final case class FlywayConfig(url: String, user: String, password: String, schema: String)
    derives ConfigReader

/** Iceberg catalog as understood by `CatalogUtil.buildIcebergCatalog`: `type` (hadoop, glue, rest,
  * jdbc, ...) plus `warehouse`, `io-impl` and catalog specific keys.
  */
final case class IcebergCatalogConfig(name: String, properties: Map[String, String])
    derives ConfigReader

final case class MigrationConfig(flyway: FlywayConfig, iceberg: IcebergCatalogConfig)
    derives ConfigReader

object MigrationConfig:
  /** Loads the `migration` section of `application.conf`; env vars override via `${?VAR}`. */
  def load(): MigrationConfig = ConfigSource.default.at("migration").loadOrThrow[MigrationConfig]
