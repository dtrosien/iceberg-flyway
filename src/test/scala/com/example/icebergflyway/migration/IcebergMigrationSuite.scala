package com.example.icebergflyway.migration

import cats.effect.*
import com.example.icebergflyway.IcebergCatalogConfig
import com.example.icebergflyway.iceberg.{IcebergCatalogs, IcebergSchemas}
import com.example.icebergflyway.iceberg.IcebergSchemas.Column
import com.example.icebergflyway.migrations.V3__Create_and_seed_countries
import com.example.icebergflyway.model.Tables
import org.apache.iceberg.{CatalogProperties, PartitionSpec, Schema}
import org.apache.iceberg.catalog.{Catalog, Namespace, SupportsNamespaces, TableIdentifier}
import com.example.icebergflyway.iceberg.IcebergData
import org.apache.iceberg.data.{GenericRecord, IcebergGenerics}
import org.apache.iceberg.inmemory.InMemoryCatalog
import org.apache.iceberg.types.Types
import weaver.*

import java.time.{Instant, ZoneOffset}
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.util.Using

/** The table helpers of IcebergMigration, exercised without Flyway. */
object IcebergMigrationSuite extends SimpleIOSuite:

  final case class V1(code: String, name: String)
  final case class V2(code: String, description: Option[String], name: String)
  final case class Address(street: String, zip: Option[String])
  final case class Person(id: Long, address: Address, previous: List[Address])
  final case class Event(id: Long, kind: String, at: Instant)

  private val table  = TableIdentifier.of("app", "countries")
  private val person = TableIdentifier.of("app", "persons")
  private val events = TableIdentifier.of("app", "events")

  // InMemoryFileIO is a JVM-wide map, so every acquisition gets its own warehouse prefix.
  private val catalog: Resource[IO, Catalog] =
    Resource.eval(IO(UUID.randomUUID())).flatMap { warehouse =>
      IcebergCatalogs.resource(
        IcebergCatalogConfig(
          "test",
          Map(
            CatalogProperties.CATALOG_IMPL       -> classOf[InMemoryCatalog].getName,
            CatalogProperties.WAREHOUSE_LOCATION -> s"memory://$warehouse"
          )
        )
      )
    }

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

  final class V7__Repartition(
      catalog: Catalog,
      partitioning: PartitionSpec.Builder => PartitionSpec.Builder
  ) extends IcebergMigration(catalog):
    def run(): Unit = evolvePartitioning(events, partitioning)

  final class V6__Create_events(catalog: Catalog) extends IcebergMigration(catalog):
    def run(): Unit =
      createTable(
        events,
        IcebergSchemas.schemaFor[Event],
        partitioning = _.identity("kind").day("at")
      )

  private def columns(catalog: Catalog, id: TableIdentifier = table): List[Column] =
    IcebergSchemas.columns(catalog.loadTable(id).schema())

  private def property(catalog: Catalog, key: String): Option[String] =
    Option(catalog.loadTable(table).properties.get(key))

  private def property(catalog: Catalog, id: TableIdentifier): Option[String] =
    Option(catalog.loadTable(id).properties.get(SchemaStamp.Property))

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

  test("createTable stamps columns and properties") {
    catalog.use { cat =>
      IO.blocking {
        V1__Create(cat, Map("comment" -> "reference data")).run()
        (property(cat, SchemaStamp.Property), property(cat, "comment"), cat.loadTable(table))
      }.map { (stamp, comment, loaded) =>
        expect(stamp.contains(SchemaStamp.describe(loaded))) and
          expect(
            stamp.exists(_.startsWith("columns: code string required, name string required;"))
          ) and
          expect(stamp.exists(_.contains("comment=reference data"))) and
          expect(comment.contains("reference data"))
      }
    }
  }

  test("createTable can be re-run after its own commit and refuses another layout") {
    catalog.use { cat =>
      for
        _     <- IO.blocking(V1__Create(cat).run())
        stamp <- IO.blocking(property(cat, SchemaStamp.Property))
        again <- IO.blocking(V1__Create(cat).run()).attempt
        same  <- IO.blocking(property(cat, SchemaStamp.Property))
        other <- IO.blocking(V2__Evolve(cat, IcebergSchemas.schemaFor[V2]).run()).attempt
        // the table now has V2's layout; re-running the V1 create must not accept it
        rerun <- IO.blocking(V1__Create(cat).run()).attempt
      yield expect(again.isRight) and expect(same == stamp) and expect(other.isRight) and
        expect(rerun.left.exists(_.isInstanceOf[IllegalStateException]))
    }
  }

  test("V3 seeds only once when re-run") {
    catalog.use { cat =>
      IO.blocking {
        V3__Create_and_seed_countries(cat).run()
        V3__Create_and_seed_countries(cat).run()
        rowCount(cat, Tables.countries)
      }.map(rows => expect(rows == 3))
    }
  }

  test("evolveTable adds new columns in case class order and restamps") {
    catalog.use { cat =>
      IO.blocking {
        V1__Create(cat).run()
        V2__Evolve(cat, IcebergSchemas.schemaFor[V2]).run()
        (columns(cat), property(cat, SchemaStamp.Property), cat.loadTable(table))
      }.map { (cols, stamp, loaded) =>
        expect.same(IcebergSchemas.columns(IcebergSchemas.schemaFor[V2]), cols) and
          expect(stamp.contains(SchemaStamp.describe(loaded)))
      }
    }
  }

  test("evolveTable is idempotent") {
    catalog.use { cat =>
      IO.blocking {
        V1__Create(cat).run()
        V2__Evolve(cat, IcebergSchemas.schemaFor[V2]).run()
        V2__Evolve(cat, IcebergSchemas.schemaFor[V2]).run()
        columns(cat)
      }.map(cols => expect.same(IcebergSchemas.columns(IcebergSchemas.schemaFor[V2]), cols))
    }
  }

  test("updateProperties sets and removes properties and restamps") {
    catalog.use { cat =>
      IO.blocking {
        V1__Create(cat, Map("comment" -> "old", "owner" -> "team-a")).run()
        V3__Properties(cat, set = Map("comment" -> "new"), remove = Set("owner")).run()
        (property(cat, "comment"), property(cat, "owner"), property(cat, SchemaStamp.Property))
      }.map { (comment, owner, stamp) =>
        expect(comment.contains("new")) and expect(owner.isEmpty) and
          expect(stamp.exists(_.contains("comment=new"))) and
          expect(stamp.forall(!_.contains("owner=")))
      }
    }
  }

  test("a column or property changed outside of migrations fails until stamped") {
    catalog.use { cat =>
      for
        _ <- IO.blocking(V1__Create(cat).run())
        _ <- IO.blocking(cat.loadTable(table).updateProperties().set("owner", "x").commit())
        evolveAfterProperty <-
          IO.blocking(V2__Evolve(cat, IcebergSchemas.schemaFor[V2]).run()).attempt
        propertiesAfterProperty <-
          IO.blocking(V3__Properties(cat, Map("a" -> "b"), Set.empty).run()).attempt
        _ <- SchemaStamp.stampAll(cat, List(table))
        _ <- IO.blocking(
          cat.loadTable(table).updateSchema().addColumn("extra", Types.StringType.get()).commit()
        )
        afterColumn <- IO.blocking(V2__Evolve(cat, IcebergSchemas.schemaFor[V2]).run()).attempt
        _           <- SchemaStamp.stampAll(cat, List(table))
        evolved     <- IO.blocking(V2__Evolve(cat, IcebergSchemas.schemaFor[V2]).run()).attempt
      yield expect(evolveAfterProperty.left.exists(_.isInstanceOf[TableDriftException])) and
        expect(propertiesAfterProperty.left.exists(_.isInstanceOf[TableDriftException])) and
        expect(afterColumn.left.exists(_.isInstanceOf[TableDriftException])) and
        expect(evolved.isRight)
    }
  }

  test("the pre-flight check fails for a drifted table and skips unmanaged or missing ones") {
    catalog.use { cat =>
      for
        _       <- IO.blocking(V1__Create(cat).run())
        _       <- IO.blocking(cat.loadTable(table).updateProperties().set("owner", "x").commit())
        drifted <- SchemaStamp.verifyAll(cat, List(table)).attempt
        _       <- SchemaStamp.stampAll(cat, List(table))
        missing = TableIdentifier.of("app", "missing")
        clean <- SchemaStamp.verifyAll(cat, List(table, missing)).attempt
      yield expect(drifted.left.exists(_.isInstanceOf[TableDriftException])) and
        expect(clean.isRight)
    }
  }

  test("the pre-flight check skips a table that is not managed yet") {
    catalog.use { cat =>
      for
        _      <- IO.blocking(createManually(cat))
        result <- SchemaStamp.verifyAll(cat, List(table)).attempt
      yield expect(result.isRight)
    }
  }

  test("adoptTable stamps a manually created table whose columns match, order aside") {
    catalog.use { cat =>
      IO.blocking {
        createManually(cat)
        V4__Adopt(cat, table, IcebergSchemas.schemaFor[V1]).run()
        val stamped = property(cat, SchemaStamp.Property).isDefined
        V2__Evolve(cat, IcebergSchemas.schemaFor[V2]).run()
        (stamped, columns(cat))
      }.map { (stamped, cols) =>
        expect(stamped) and
          expect.same(IcebergSchemas.columns(IcebergSchemas.schemaFor[V2]), cols)
      }
    }
  }

  test("adoptTable refuses a table that does not look like the declared schema") {
    catalog.use { cat =>
      for
        _      <- IO.blocking(createManually(cat))
        result <- IO.blocking(V4__Adopt(cat, table, IcebergSchemas.schemaFor[V2]).run()).attempt
        stamp  <- IO.blocking(property(cat, SchemaStamp.Property))
      yield expect(result.left.exists(_.isInstanceOf[IllegalStateException])) and
        expect(stamp.isEmpty)
    }
  }

  test("createTable sets partition fields, append writes one file per partition") {
    catalog.use { cat =>
      IO.blocking {
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
        (property(cat, events), partitions, rowCount(cat, events))
      }.map { (stamp, partitions, rows) =>
        expect(stamp.exists(_.contains("; partitions: identity(kind), day(at); "))) and
          expect.same(
            List("kind=click/at_day=2026-01-01", "kind=view/at_day=2026-01-02"),
            partitions
          ) and
          expect(rows == 3)
      }
    }
  }

  test("evolvePartitioning removes, adds and renames fields declaratively and is idempotent") {
    catalog.use { cat =>
      IO.blocking {
        V6__Create_events(cat).run()
        // drop identity(kind), keep day(at) under a new name, add bucket(id)
        val target: PartitionSpec.Builder => PartitionSpec.Builder =
          _.day("at", "event_day").bucket("id", 4)
        V7__Repartition(cat, target).run()
        V7__Repartition(cat, target).run()
        val table  = cat.loadTable(events)
        val fields = table.spec().fields().asScala.toList.map(_.name())
        (SchemaStamp.partitions(table.schema(), table.spec()), fields, property(cat, events))
      }.map { (partitions, names, stamp) =>
        expect.same(List("day(at)", "bucket[4](id)"), partitions) and
          expect.same(List("event_day", "id_bucket"), names) and
          expect(stamp.exists(_.contains("; partitions: day(at), bucket[4](id); ")))
      }
    }
  }

  test("a partition spec changed outside of migrations is drift") {
    catalog.use { cat =>
      for
        _      <- IO.blocking(V6__Create_events(cat).run())
        _      <- IO.blocking(cat.loadTable(events).updateSpec().addField("id").commit())
        result <- SchemaStamp.verifyAll(cat, List(events)).attempt
      yield expect(result.left.exists(_.isInstanceOf[TableDriftException]))
    }
  }

  test("nested case classes compare id-free: a table created from the model adopts as the model") {
    catalog.use { cat =>
      IO.blocking {
        V5__Create_persons(cat).run()
        V4__Adopt(cat, person, IcebergSchemas.schemaFor[Person]).run()
        columns(cat, person)
      }.map(cols => expect.same(IcebergSchemas.columns(IcebergSchemas.schemaFor[Person]), cols))
    }
  }

  test("an unmanaged table cannot be evolved or get properties before it is adopted") {
    catalog.use { cat =>
      for
        _          <- IO.blocking(createManually(cat))
        evolve     <- IO.blocking(V2__Evolve(cat, IcebergSchemas.schemaFor[V2]).run()).attempt
        properties <- IO.blocking(V3__Properties(cat, Map("owner" -> "x"), Set.empty).run()).attempt
      yield expect(evolve.left.exists(_.isInstanceOf[TableDriftException])) and
        expect(properties.left.exists(_.isInstanceOf[TableDriftException]))
    }
  }
