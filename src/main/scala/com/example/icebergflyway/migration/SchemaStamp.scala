package com.example.icebergflyway.migration

import com.example.icebergflyway.iceberg.IcebergSchemas
import org.apache.iceberg.{PartitionSpec, Schema, Table, Transaction}
import org.apache.iceberg.catalog.{Catalog, TableIdentifier}

import scala.jdk.CollectionConverters.*

/** Records the column layout, partitioning and properties a migration left behind in the table
  * property `iceberg-flyway.schema` and fails when a table no longer matches it, i.e. when it was
  * changed outside of migrations. A table without the property is not managed yet (see
  * `adoptTable`).
  */
object SchemaStamp:
  val Property = "iceberg-flyway.schema"

  /** Partition fields as `transform(source column)`, id-free. */
  def partitions(schema: Schema, spec: PartitionSpec): List[String] =
    spec.fields.asScala.toList.map(f => s"${f.transform}(${schema.findColumnName(f.sourceId)})")

  /** Canonical, id-free description: columns in table order, partition fields, sorted properties.
    */
  def describe(schema: Schema, spec: PartitionSpec, properties: Map[String, String]): String =
    val columns = IcebergSchemas
      .columns(schema)
      .map(c => s"${c.name} ${c.`type`} ${if c.optional then "optional" else "required"}")
    val props = properties.removed(Property).toList.sorted.map((k, v) => s"$k=$v")
    s"columns: ${columns.mkString(", ")}; partitions: ${partitions(schema, spec).mkString(", ")}; " +
      s"properties: ${props.mkString(", ")}"

  def describe(table: Table): String =
    describe(table.schema(), table.spec(), table.properties().asScala.toMap)

  def isStamped(table: Table): Boolean = table.properties().containsKey(Property)

  def stamp(tx: Transaction): Unit =
    tx.updateProperties().set(Property, describe(tx.table())).commit()

  def stamp(table: Table): Unit =
    table.updateProperties().set(Property, describe(table)).commit()

  def verify(id: TableIdentifier, table: Table): Unit =
    val recorded = Option(table.properties.get(Property))
    val actual   = describe(table)
    if !recorded.contains(actual) then throw TableDriftException(id, recorded, actual)

  /** Pre-flight check for every registered table that exists and is managed. */
  def verifyAll(catalog: Catalog, tables: List[TableIdentifier]): Unit =
    tables.foreach { id =>
      if catalog.tableExists(id) then
        val table = catalog.loadTable(id)
        if isStamped(table) then verify(id, table)
    }

  /** Accepts the current state of the tables as the migrated state (the repair path). */
  def stampAll(catalog: Catalog, tables: List[TableIdentifier]): Unit =
    tables.foreach(id => stamp(catalog.loadTable(id)))

final class TableDriftException(id: TableIdentifier, recorded: Option[String], actual: String)
    extends RuntimeException(
      recorded.fold(
        s"Table $id is not managed by migrations yet (no ${SchemaStamp.Property} property). " +
          s"Adopt it with adoptTable in a migration, or run `stamp $id`."
      )(expected =>
        s"Table $id was changed outside of migrations. Recorded: $expected. Actual: $actual. " +
          s"Revert the change, or codify it in a new migration and run `stamp $id` in this " +
          "environment before migrating."
      )
    )
