package com.example.icebergflyway.migration

import com.example.icebergflyway.{FlywayConfig, UnitSpec}
import com.example.icebergflyway.iceberg.IcebergSchemas
import com.example.icebergflyway.migrations.Migrations
import com.example.icebergflyway.model.{Country, Tables, User}
import org.apache.iceberg.{CatalogProperties, CatalogUtil, Table}
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.data.{IcebergGenerics, Record}
import org.apache.iceberg.inmemory.InMemoryCatalog
import org.flywaydb.core.api.MigrationState
import org.flywaydb.core.api.output.MigrateResult
import org.scalatest.BeforeAndAfterAll
import org.testcontainers.postgresql.PostgreSQLContainer

import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.util.Using

/** Runs every migration against Postgres (Flyway history) and an in-memory Iceberg catalog. */
class MigrationTest extends UnitSpec with BeforeAndAfterAll:

  private val postgres = PostgreSQLContainer("postgres:alpine")

  // InMemoryFileIO is a JVM-wide map, so every catalog gets its own warehouse prefix.
  private def newCatalog(): Catalog =
    CatalogUtil.buildIcebergCatalog(
      "test",
      Map(
        CatalogProperties.CATALOG_IMPL       -> classOf[InMemoryCatalog].getName,
        CatalogProperties.WAREHOUSE_LOCATION -> s"memory://${UUID.randomUUID()}"
      ).asJava,
      null
    )

  private def flyway(schema: String, catalog: Catalog): IcebergFlyway =
    IcebergFlyway(
      FlywayConfig(postgres.getJdbcUrl, postgres.getUsername, postgres.getPassword, schema),
      Migrations.all(catalog)
    )

  private lazy val catalog: Catalog          = newCatalog()
  private lazy val migrations: IcebergFlyway = flyway("iceberg_flyway", catalog)
  private var firstRun: MigrateResult        = scala.compiletime.uninitialized

  override def beforeAll(): Unit =
    postgres.start()
    firstRun = migrations.migrate()

  override def afterAll(): Unit =
    catalog match
      case closeable: AutoCloseable => closeable.close()
      case _                        => ()
    postgres.stop()

  private def readAll(table: Table): List[Record] =
    Using.resource(IcebergGenerics.read(table).build())(_.asScala.toList)

  "migrate" should "apply all migrations and record them in the history table" in {
    // Flyway also records an unversioned row for creating its own schema.
    val applied = migrations
      .info()
      .applied()
      .toList
      .filter(_.isVersioned)
      .map(i => (i.getVersion.toString, i.getState))

    firstRun.migrationsExecuted shouldBe 4
    applied shouldBe List("1", "2", "3", "4").map(_ -> MigrationState.SUCCESS)
  }

  it should "leave the users table matching the current User model, including column order" in {
    val schema = catalog.loadTable(Tables.users).schema()

    IcebergSchemas.columns(schema) shouldBe IcebergSchemas.columns(IcebergSchemas.schemaFor[User])
  }

  it should "leave the countries table matching the current Country model" in {
    val schema = catalog.loadTable(Tables.countries).schema()

    IcebergSchemas.columns(schema) shouldBe IcebergSchemas.columns(
      IcebergSchemas.schemaFor[Country]
    )
  }

  it should "partition users by day(created_at) (V1)" in {
    val table = catalog.loadTable(Tables.users)

    SchemaStamp.partitions(table.schema(), table.spec()) shouldBe List("day(created_at)")
  }

  it should "stamp every managed table with the layout of its last migration" in {
    Tables.all.map(catalog.loadTable).foreach { table =>
      Option(table.properties.get(SchemaStamp.Property)).value shouldBe SchemaStamp.describe(table)
    }
  }

  it should "set the users table properties (V4)" in {
    val props = catalog.loadTable(Tables.users).properties()

    props.get("write.target-file-size-bytes") shouldBe "134217728"
    props.get("history.expire.max-snapshot-age-ms") shouldBe "604800000"
  }

  it should "seed the countries table (V3)" in {
    val seeded = readAll(catalog.loadTable(Tables.countries))
      .map(r => (r.getField("code").toString, r.getField("name").toString))
      .sorted

    seeded shouldBe List("DE" -> "Germany", "FR" -> "France", "US" -> "United States")
  }

  // Flyway refuses a non-empty schema without a history table, so sets sharing a database
  // need their own schema rather than their own history table.
  it should "let a second migration set with its own schema share the database" in {
    val other = newCatalog()
    try flyway("other_flyway", other).migrate().migrationsExecuted shouldBe 4
    finally other.asInstanceOf[AutoCloseable].close()
  }

  it should "be a no-op the second time, and validate passes" in {
    migrations.migrate().migrationsExecuted shouldBe 0
    noException should be thrownBy migrations.validate()
  }
