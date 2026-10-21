package com.example.icebergflyway.migrations

import com.example.icebergflyway.iceberg.IcebergSchemas
import com.example.icebergflyway.migration.IcebergMigration
import com.example.icebergflyway.model.{Tables, User}
import org.apache.iceberg.catalog.Catalog

/** Brings `users` in line with the current `User` model, which adds the optional `email`. */
final class V2__Add_user_email(catalog: Catalog) extends IcebergMigration(catalog):
  def run(): Unit = evolveTable(Tables.users, IcebergSchemas.schemaFor[User])
