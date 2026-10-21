package com.example.icebergflyway.migrations

import com.example.icebergflyway.migration.IcebergMigration
import com.example.icebergflyway.model.Tables
import org.apache.iceberg.catalog.Catalog

/** Table properties are versioned like columns: smaller data files and a one week snapshot
  * retention for `users`.
  */
final class V4__Tune_users_table_properties(catalog: Catalog) extends IcebergMigration(catalog):
  def run(): Unit = updateProperties(
    Tables.users,
    set = Map(
      "write.target-file-size-bytes"       -> (128L * 1024 * 1024).toString,
      "history.expire.max-snapshot-age-ms" -> (7L * 24 * 60 * 60 * 1000).toString
    )
  )
