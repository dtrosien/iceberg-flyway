package com.example.icebergflyway.migration

import com.example.icebergflyway.iceberg.IcebergSchemas
import org.apache.iceberg.{PartitionField, PartitionSpec, Schema, Table}
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.transforms.Transform
import org.apache.iceberg.catalog.{Catalog, Namespace, SupportsNamespaces, TableIdentifier}
import org.flywaydb.core.api.migration.{BaseJavaMigration, Context}

import scala.jdk.CollectionConverters.*

/** A Flyway Java migration that changes Iceberg tables instead of the JDBC database. Flyway only
  * records it in the schema history table and serialises runs with its advisory lock.
  *
  * Subclasses follow Flyway's naming scheme (`V3__Add_user_email` is version 3, "Add user email")
  * and take the catalog as constructor argument; instances are registered in `Migrations`. Flyway
  * invokes `run()` synchronously, so it is a plain side effect by contract.
  *
  * Flyway's Postgres transaction cannot roll back Iceberg commits: when a run dies after an Iceberg
  * commit but before the history row is written, Flyway re-runs the migration. Every helper is
  * therefore safe to re-run: it first checks the table's [[SchemaStamp]] and stamps the result in
  * the same Iceberg transaction as the change.
  */
abstract class IcebergMigration(protected val catalog: Catalog) extends BaseJavaMigration:

  def run(): Unit

  final override def migrate(context: Context): Unit = run()

  protected def ensureNamespace(namespace: Namespace): Unit = catalog match
    case c: SupportsNamespaces if !c.namespaceExists(namespace) => c.createNamespace(namespace)
    case _                                                      => ()

  /** Creates a table (and its namespace if the catalog needs one) with the given table properties
    * and partitioning, stamped with its layout. `partitioning` receives a builder for `schema`,
    * e.g. `_.identity("country").day("created_at")`; the default is unpartitioned. On a re-run the
    * table already exists: it is accepted when it carries a matching stamp and the expected columns
    * and partitioning, otherwise this fails.
    */
  protected def createTable(
      id: TableIdentifier,
      schema: Schema,
      properties: Map[String, String] = Map.empty,
      partitioning: PartitionSpec.Builder => PartitionSpec.Builder = builder => builder
  ): Table =
    val spec = partitioning(PartitionSpec.builderFor(schema)).build()
    if catalog.tableExists(id) then
      val table = catalog.loadTable(id)
      SchemaStamp.verify(id, table)
      val sameColumns    = IcebergSchemas.columns(table.schema()) == IcebergSchemas.columns(schema)
      val samePartitions = SchemaStamp.partitions(table.schema(), table.spec()) ==
        SchemaStamp.partitions(schema, spec)
      if !sameColumns || !samePartitions then
        throw IllegalStateException(
          s"Table $id already exists with another layout: ${SchemaStamp.describe(table)}"
        )
      table
    else
      ensureNamespace(id.namespace)
      val tx = catalog
        .buildTable(id, schema)
        .withPartitionSpec(spec)
        .withProperties(properties.asJava)
        .createTransaction()
      SchemaStamp.stamp(tx)
      tx.commitTransaction()
      catalog.loadTable(id)

  /** Takes over a table that was created outside of migrations. Its columns must match `expected`
    * by name, type and optionality (order is ignored; a following `evolveTable` fixes it), then the
    * table is stamped. Fails when the table does not exist or looks different.
    */
  protected def adoptTable(id: TableIdentifier, expected: Schema): Table =
    val table  = catalog.loadTable(id)
    val actual = IcebergSchemas.columns(table.schema())
    val wanted = IcebergSchemas.columns(expected)
    if actual.toSet != wanted.toSet then
      throw IllegalStateException(
        s"Cannot adopt table $id: expected columns ${wanted.mkString(", ")} but found " +
          actual.mkString(", ")
      )
    SchemaStamp.stamp(table)
    catalog.loadTable(id)

  /** Brings the table in line with `schema`: adds missing columns (as optional), relaxes required
    * columns that became optional and orders the columns like `schema`; columns no longer in
    * `schema` stay at the end. It never drops, renames or narrows columns. Fails with
    * [[TableDriftException]] when the table was changed outside of migrations.
    */
  protected def evolveTable(id: TableIdentifier, schema: Schema): Table =
    val table = catalog.loadTable(id)
    SchemaStamp.verify(id, table)
    val tx = table.newTransaction()
    tx.updateSchema().unionByNameWith(schema).commit()
    val reorder = tx.updateSchema()
    schema.columns.asScala.map(_.name).foldLeft(Option.empty[String]) { (previous, name) =>
      previous.fold(reorder.moveFirst(name))(reorder.moveAfter(name, _))
      Some(name)
    }
    reorder.commit()
    SchemaStamp.stamp(tx)
    tx.commitTransaction()
    catalog.loadTable(id)

  /** Brings the partition spec in line with `partitioning`, declared like in `createTable` but
    * against the table's current schema. Fields are matched by transform and source column: fields
    * missing from the target are removed, new ones are added (at the end), and a field whose only
    * difference is its name is renamed. Existing data files keep their old layout.
    */
  protected def evolvePartitioning(
      id: TableIdentifier,
      partitioning: PartitionSpec.Builder => PartitionSpec.Builder
  ): Table =
    val table = catalog.loadTable(id)
    SchemaStamp.verify(id, table)
    val schema  = table.schema()
    val current = table.spec().fields().asScala.toList
    val target  = partitioning(PartitionSpec.builderFor(schema)).build().fields().asScala.toList
    def key(field: PartitionField) = (field.transform().toString, field.sourceId())
    val tx                         = table.newTransaction()
    val update                     = tx.updateSpec()
    current
      .filterNot(f => target.exists(key(_) == key(f)))
      .foreach(f => update.removeField(f.name()))
    target.foreach { wanted =>
      current.find(key(_) == key(wanted)) match
        case None =>
          val source = schema.findColumnName(wanted.sourceId())
          val term   =
            Expressions.transform(source, wanted.transform().asInstanceOf[Transform[Any, Any]])
          update.addField(wanted.name(), term)
        case Some(existing) if existing.name() != wanted.name() =>
          update.renameField(existing.name(), wanted.name())
        case Some(_) => ()
    }
    update.commit()
    SchemaStamp.stamp(tx)
    tx.commitTransaction()
    catalog.loadTable(id)

  /** Sets and removes table properties in one commit. */
  protected def updateProperties(
      id: TableIdentifier,
      set: Map[String, String],
      remove: Set[String] = Set.empty
  ): Table =
    val table = catalog.loadTable(id)
    SchemaStamp.verify(id, table)
    val tx     = table.newTransaction()
    val update = tx.updateProperties()
    set.foreach((key, value) => update.set(key, value))
    remove.foreach(update.remove)
    update.commit()
    SchemaStamp.stamp(tx)
    tx.commitTransaction()
    catalog.loadTable(id)
