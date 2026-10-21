package com.example.icebergflyway.migration

import com.example.icebergflyway.{IcebergCatalogConfig, UnitSpec}
import com.example.icebergflyway.iceberg.{IcebergCatalogs, IcebergData, IcebergSchemas}
import com.example.icebergflyway.iceberg.IcebergSchemas.Column
import com.example.icebergflyway.migrations.V3__Create_and_seed_countries
import com.example.icebergflyway.model.Tables
import org.apache.iceberg.{CatalogProperties, PartitionSpec, Schema}
import org.apache.iceberg.catalog.{Catalog, Namespace, SupportsNamespaces, TableIdentifier}
import org.apache.iceberg.data.{GenericRecord, IcebergGenerics}
import org.apache.iceberg.inmemory.InMemoryCatalog
import org.apache.iceberg.types.Types

import java.time.{Instant, ZoneOffset}
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.util.Using

/** The table helpers of IcebergMigration, exercised without Flyway. */
class IcebergMigrationTest extends UnitSpec:

  final case class V1(code: String, name: String)
  final case class V2(code: String, description: Option[String], name: String)
  final case class Address(street: String, zip: Option[String])
  final case class Person(id: Long, address: Address, previous: List[Address])
  final case class Event(id: Long, kind: String, at: Instant)

  private val table  = TableIdentifier.of("app", "countries")
  private val person = TableIdentifier.of("app", "persons")
  private val events = TableIdentifier.of("app", "events")

  // InMemoryFileIO is a JVM-wide map, so every test gets its own warehouse prefix.
  private def withCatalog[A](test: Catalog => A): A =
    val config = IcebergCatalogConfig(
      "test",
      Map(
        CatalogProperties.CATALOG_IMPL       -> classOf[InMemoryCatalog].getName,
        CatalogProperties.WAREHOUSE_LOCATION -> s"memory://${UUID.randomUUID()}"
      )
    )
    IcebergCatalogs.withCatalog(config)(test)

  // Flyway's naming scheme applies to any BaseJavaMigration, hence the V prefixes.
  final class V1__Create(catalog: Catalog, properties: Map[String, String] = Map.empty)
      extends IcebergMigration(catalog):
    def run(): Unit = createTable(table, IcebergSchemas.schemaFor[V1], properties)

  final class V2__Evolve(catalog: Catalog, schema: Schema) extends IcebergMigration(catalog):
    def run(): Unit = evolveTable(table, schema)

  final class V3__Properties(catalog: Catalog, set: Map[String, String], remove: Set[String])
      extends IcebergMigration(catalog):
    def run(): Unit = updateProperties(table, set, remove)

  final class V4__Adopt(catalog: Catalog, id: TableIdentifier, expected: Schema)
      extends IcebergMigration(catalog):
    def run(): Unit = adoptTable(id, expected)

  final class V5__Create_persons(catalog: Catalog) extends IcebergMigration(catalog):
    def run(): Unit = createTable(person, IcebergSchemas.schemaFor[Person])

  final class V6__Create_events(catalog: Catalog) extends IcebergMigration(catalog):
    def run(): Unit =
      createTable(
        events,
        IcebergSchemas.schemaFor[Event],
        partitioning = _.identity("kind").day("at")
      )

  final class V7__Repartition(
      catalog: Catalog,
      partitioning: PartitionSpec.Builder => PartitionSpec.Builder
  ) extends IcebergMigration(catalog):
    def run(): Unit = evolvePartitioning(events, partitioning)

  private def columns(catalog: Catalog, id: TableIdentifier = table): List[Column] =
    IcebergSchemas.columns(catalog.loadTable(id).schema())

  private def property(catalog: Catalog, key: String, id: TableIdentifier = table): Option[String] =
    Option(catalog.loadTable(id).properties.get(key))

  private def stamp(catalog: Catalog, id: TableIdentifier = table): Option[String] =
    property(catalog, SchemaStamp.Property, id)

  private def rowCount(catalog: Catalog, id: TableIdentifier): Int =
    Using.resource(IcebergGenerics.read(catalog.loadTable(id)).build())(_.asScala.size)

  /** A table created the way Spark or Athena would: no stamp, columns in another order. */
  private def createManually(catalog: Catalog): Unit =
    catalog.asInstanceOf[SupportsNamespaces].createNamespace(Namespace.of("app"))
    catalog.createTable(
      table,
      Schema(
        Types.NestedField.required(1, "name", Types.StringType.get()),
        Types.NestedField.required(2, "code", Types.StringType.get())
      )
    )

  "createTable" should "stamp columns and properties" in withCatalog { cat =>
    V1__Create(cat, Map("comment" -> "reference data")).run()

    stamp(cat).value shouldBe SchemaStamp.describe(cat.loadTable(table))
    stamp(cat).value should startWith("columns: code string required, name string required;")
    stamp(cat).value should include("comment=reference data")
    property(cat, "comment").value shouldBe "reference data"
  }

  it should "be re-runnable after its own commit and refuse another layout" in withCatalog { cat =>
    V1__Create(cat).run()
    val first = stamp(cat)

    noException should be thrownBy V1__Create(cat).run()
    stamp(cat) shouldBe first

    V2__Evolve(cat, IcebergSchemas.schemaFor[V2]).run()
    // the table now has V2's layout; re-running the V1 create must not accept it
    an[IllegalStateException] should be thrownBy V1__Create(cat).run()
  }

  it should "set partition fields, and append writes one file per partition" in withCatalog { cat =>
    V6__Create_events(cat).run()
    val table = cat.loadTable(events)
    val day1  = Instant.parse("2026-01-01T10:00:00Z").atOffset(ZoneOffset.UTC)
    val day2  = Instant.parse("2026-01-02T10:00:00Z").atOffset(ZoneOffset.UTC)
    val rows  = List((1L, "click", day1), (2L, "click", day1), (3L, "view", day2)).map {
      (id, kind, at) =>
        val row = GenericRecord.create(table.schema())
        row.setField("id", id)
        row.setField("kind", kind)
        row.setField("at", at)
        row
    }

    IcebergData.append(table, rows)

    val files      = Using.resource(table.newScan().planFiles())(_.asScala.toList)
    val partitions = files.map(f => table.spec.partitionToPath(f.file.partition)).sorted
    partitions shouldBe List("kind=click/at_day=2026-01-01", "kind=view/at_day=2026-01-02")
    rowCount(cat, events) shouldBe 3
    stamp(cat, events).value should include("; partitions: identity(kind), day(at); ")
  }

  "V3" should "seed only once when re-run" in withCatalog { cat =>
    V3__Create_and_seed_countries(cat).run()
    V3__Create_and_seed_countries(cat).run()

    rowCount(cat, Tables.countries) shouldBe 3
  }

  "evolveTable" should "add new columns in case class order and restamp" in withCatalog { cat =>
    V1__Create(cat).run()
    V2__Evolve(cat, IcebergSchemas.schemaFor[V2]).run()

    columns(cat) shouldBe IcebergSchemas.columns(IcebergSchemas.schemaFor[V2])
    stamp(cat).value shouldBe SchemaStamp.describe(cat.loadTable(table))
  }

  it should "be idempotent" in withCatalog { cat =>
    V1__Create(cat).run()
    V2__Evolve(cat, IcebergSchemas.schemaFor[V2]).run()
    V2__Evolve(cat, IcebergSchemas.schemaFor[V2]).run()

    columns(cat) shouldBe IcebergSchemas.columns(IcebergSchemas.schemaFor[V2])
  }

  "evolvePartitioning" should "remove, add and rename fields declaratively and be idempotent" in withCatalog {
    cat =>
      V6__Create_events(cat).run()
      // drop identity(kind), keep day(at) under a new name, add bucket(id)
      val target: PartitionSpec.Builder => PartitionSpec.Builder =
        _.day("at", "event_day").bucket("id", 4)

      V7__Repartition(cat, target).run()
      V7__Repartition(cat, target).run()

      val table = cat.loadTable(events)
      SchemaStamp.partitions(table.schema(), table.spec()) shouldBe List("day(at)", "bucket[4](id)")
      table.spec().fields().asScala.toList.map(_.name()) shouldBe List("event_day", "id_bucket")
      stamp(cat, events).value should include("; partitions: day(at), bucket[4](id); ")
  }

  "updateProperties" should "set and remove properties and restamp" in withCatalog { cat =>
    V1__Create(cat, Map("comment" -> "old", "owner" -> "team-a")).run()
    V3__Properties(cat, set = Map("comment" -> "new"), remove = Set("owner")).run()

    property(cat, "comment").value shouldBe "new"
    property(cat, "owner") shouldBe None
    stamp(cat).value should include("comment=new")
    stamp(cat).value should not include "owner="
  }

  "SchemaStamp" should "fail evolve and property changes after a change outside of migrations until stamped" in
    withCatalog { cat =>
      V1__Create(cat).run()
      cat.loadTable(table).updateProperties().set("owner", "x").commit()

      a[TableDriftException] should be thrownBy V2__Evolve(cat, IcebergSchemas.schemaFor[V2]).run()
      a[TableDriftException] should be thrownBy V3__Properties(cat, Map("a" -> "b"), Set.empty)
        .run()

      SchemaStamp.stampAll(cat, List(table))
      cat.loadTable(table).updateSchema().addColumn("extra", Types.StringType.get()).commit()
      a[TableDriftException] should be thrownBy V2__Evolve(cat, IcebergSchemas.schemaFor[V2]).run()

      SchemaStamp.stampAll(cat, List(table))
      noException should be thrownBy V2__Evolve(cat, IcebergSchemas.schemaFor[V2]).run()
    }

  it should "detect a partition spec changed outside of migrations" in withCatalog { cat =>
    V6__Create_events(cat).run()
    cat.loadTable(events).updateSpec().addField("id").commit()

    a[TableDriftException] should be thrownBy SchemaStamp.verifyAll(cat, List(events))
  }

  it should "fail the pre-flight check for a drifted table and skip unmanaged or missing ones" in withCatalog {
    cat =>
      V1__Create(cat).run()
      cat.loadTable(table).updateProperties().set("owner", "x").commit()
      a[TableDriftException] should be thrownBy SchemaStamp.verifyAll(cat, List(table))

      SchemaStamp.stampAll(cat, List(table))
      noException should be thrownBy SchemaStamp.verifyAll(
        cat,
        List(table, TableIdentifier.of("app", "missing"))
      )
  }

  it should "skip a table that is not managed yet in the pre-flight check" in withCatalog { cat =>
    createManually(cat)

    noException should be thrownBy SchemaStamp.verifyAll(cat, List(table))
  }

  "adoptTable" should "stamp a manually created table whose columns match, order aside" in withCatalog {
    cat =>
      createManually(cat)
      V4__Adopt(cat, table, IcebergSchemas.schemaFor[V1]).run()
      stamp(cat) shouldBe defined

      V2__Evolve(cat, IcebergSchemas.schemaFor[V2]).run()
      columns(cat) shouldBe IcebergSchemas.columns(IcebergSchemas.schemaFor[V2])
  }

  it should "refuse a table that does not look like the declared schema" in withCatalog { cat =>
    createManually(cat)

    an[IllegalStateException] should be thrownBy V4__Adopt(cat, table, IcebergSchemas.schemaFor[V2])
      .run()
    stamp(cat) shouldBe None
  }

  it should "compare nested case classes id-free, so a table created from the model adopts as the model" in
    withCatalog { cat =>
      V5__Create_persons(cat).run()
      V4__Adopt(cat, person, IcebergSchemas.schemaFor[Person]).run()

      columns(cat, person) shouldBe IcebergSchemas.columns(IcebergSchemas.schemaFor[Person])
    }

  "An unmanaged table" should "not be evolved or get properties before it is adopted" in withCatalog {
    cat =>
      createManually(cat)

      a[TableDriftException] should be thrownBy V2__Evolve(cat, IcebergSchemas.schemaFor[V2]).run()
      a[TableDriftException] should be thrownBy V3__Properties(cat, Map("owner" -> "x"), Set.empty)
        .run()
  }
