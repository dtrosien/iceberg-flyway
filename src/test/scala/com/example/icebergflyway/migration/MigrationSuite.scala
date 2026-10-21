package com.example.icebergflyway.migration

import cats.effect.*
import com.example.icebergflyway.{FlywayConfig, IcebergCatalogConfig}
import com.example.icebergflyway.iceberg.{IcebergCatalogs, IcebergSchemas}
import com.example.icebergflyway.migrations.Migrations
import com.example.icebergflyway.model.{Country, Tables, User}
import org.apache.iceberg.{CatalogProperties, Table}
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.data.{IcebergGenerics, Record}
import org.apache.iceberg.inmemory.InMemoryCatalog
import org.flywaydb.core.api.MigrationState
import org.flywaydb.core.api.output.MigrateResult
import org.testcontainers.postgresql.PostgreSQLContainer
import weaver.*

import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.util.Using

/** Runs every migration against Postgres (Flyway history) and an in-memory Iceberg catalog. */
object MigrationSuite extends IOSuite:

  final case class Env(
      postgres: PostgreSQLContainer,
      flyway: IcebergFlyway,
      catalog: Catalog,
      firstRun: MigrateResult
  )
  type Res = Env

  override def maxParallelism: Int = 1

  private val postgres: Resource[IO, PostgreSQLContainer] =
    Resource.make(IO.blocking {
      val container = PostgreSQLContainer("postgres:alpine")
      container.start()
      container
    })(container => IO.blocking(container.stop()))

  // InMemoryFileIO is a JVM-wide map, so every catalog gets its own warehouse prefix.
  private val catalog: Resource[IO, Catalog] = IcebergCatalogs.resource(
    IcebergCatalogConfig(
      "test",
      Map(
        CatalogProperties.CATALOG_IMPL       -> classOf[InMemoryCatalog].getName,
        CatalogProperties.WAREHOUSE_LOCATION -> s"memory://${UUID.randomUUID()}"
      )
    )
  )

  private def flyway(pg: PostgreSQLContainer, schema: String, cat: Catalog): IO[IcebergFlyway] =
    IcebergFlyway(
      FlywayConfig(pg.getJdbcUrl, pg.getUsername, pg.getPassword, schema),
      Migrations.all(cat)
    )

  def sharedResource: Resource[IO, Res] =
    for
      pg     <- postgres
      cat    <- catalog
      fw     <- Resource.eval(flyway(pg, "iceberg_flyway", cat))
      result <- Resource.eval(fw.migrate)
    yield Env(pg, fw, cat, result)

  private def readAll(table: Table): List[Record] =
    Using.resource(IcebergGenerics.read(table).build())(_.asScala.toList)

  test("all migrations are applied and recorded in the history table") { env =>
    env.flyway.info.map { info =>
      // Flyway also records an unversioned row for creating its own schema.
      val applied =
        info.applied().toList.filter(_.isVersioned).map(i => (i.getVersion.toString, i.getState))
      expect(env.firstRun.migrationsExecuted == 4) and
        expect.same(List("1", "2", "3", "4").map(_ -> MigrationState.SUCCESS), applied)
    }
  }

  test("the users table matches the current User model, including column order") { env =>
    IO.blocking(env.catalog.loadTable(Tables.users).schema()).map { schema =>
      expect.same(
        IcebergSchemas.columns(IcebergSchemas.schemaFor[User]),
        IcebergSchemas.columns(schema)
      )
    }
  }

  test("V1 partitioned users by day(created_at)") { env =>
    IO.blocking(env.catalog.loadTable(Tables.users)).map { table =>
      expect.same(List("day(created_at)"), SchemaStamp.partitions(table.schema(), table.spec()))
    }
  }

  test("every managed table carries the schema stamp of its last migration") { env =>
    IO.blocking(Tables.all.map(env.catalog.loadTable)).map { tables =>
      forEach(tables) { table =>
        expect.same(
          Some(SchemaStamp.describe(table)),
          Option(table.properties.get(SchemaStamp.Property))
        )
      }
    }
  }

  test("the countries table matches the current Country model") { env =>
    IO.blocking(env.catalog.loadTable(Tables.countries).schema()).map { schema =>
      expect.same(
        IcebergSchemas.columns(IcebergSchemas.schemaFor[Country]),
        IcebergSchemas.columns(schema)
      )
    }
  }

  test("V4 set the users table properties") { env =>
    IO.blocking(env.catalog.loadTable(Tables.users).properties()).map { props =>
      expect(props.get("write.target-file-size-bytes") == "134217728") and
        expect(props.get("history.expire.max-snapshot-age-ms") == "604800000")
    }
  }

  test("V3 seeded the countries table") { env =>
    IO.blocking(readAll(env.catalog.loadTable(Tables.countries))).map { rows =>
      val seeded = rows.map(r => (r.getField("code").toString, r.getField("name").toString)).sorted
      expect.same(List("DE" -> "Germany", "FR" -> "France", "US" -> "United States"), seeded)
    }
  }

  // Flyway refuses a non-empty schema without a history table, so sets sharing a database
  // need their own schema rather than their own history table.
  test("a second migration set with its own schema can share the database") { env =>
    catalog.use { other =>
      flyway(env.postgres, "other_flyway", other).flatMap(_.migrate).map { result =>
        expect(result.migrationsExecuted == 4)
      }
    }
  }

  test("a second migrate is a no-op and validate passes") { env =>
    for
      result <- env.flyway.migrate
      _      <- env.flyway.validate
    yield expect(result.migrationsExecuted == 0)
  }
