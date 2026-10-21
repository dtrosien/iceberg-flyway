package com.example.icebergflyway.migrations

import org.apache.iceberg.catalog.Catalog
import org.flywaydb.core.api.migration.JavaMigration

object Migrations:
  /** Every migration of this project; Flyway orders them by version. Register new ones here. */
  def all(catalog: Catalog): List[JavaMigration] = List(
    V1__Create_users_table(catalog),
    V2__Add_user_email(catalog),
    V3__Create_and_seed_countries(catalog),
    V4__Tune_users_table_properties(catalog)
  )
