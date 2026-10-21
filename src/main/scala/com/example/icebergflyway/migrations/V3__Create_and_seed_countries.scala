package com.example.icebergflyway.migrations

import com.example.icebergflyway.iceberg.{IcebergData, IcebergSchemas}
import com.example.icebergflyway.migration.IcebergMigration
import com.example.icebergflyway.model.Tables
import org.apache.iceberg.Table
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.data.{GenericRecord, Record}

/** `countries` as it looked when this migration was written (see `UserV1`). */
private final case class CountryV3(code: String, name: String)

/** Creates the `countries` reference table and seeds it. */
final class V3__Create_and_seed_countries(catalog: Catalog) extends IcebergMigration(catalog):

  private val seed =
    List(CountryV3("DE", "Germany"), CountryV3("FR", "France"), CountryV3("US", "United States"))

  def run(): Unit =
    val table = createTable(Tables.countries, IcebergSchemas.schemaFor[CountryV3])
    // Re-run safety: a retry after a failed Flyway commit must not seed twice.
    if table.currentSnapshot() == null then IcebergData.append(table, seed.map(record(table, _)))

  private def record(table: Table, country: CountryV3): Record =
    val row = GenericRecord.create(table.schema())
    row.setField("code", country.code)
    row.setField("name", country.name)
    row
