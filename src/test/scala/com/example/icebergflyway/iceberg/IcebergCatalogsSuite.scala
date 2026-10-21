package com.example.icebergflyway.iceberg

import cats.effect.*
import com.example.icebergflyway.IcebergCatalogConfig
import com.example.icebergflyway.model.{Tables, User}
import org.apache.iceberg.CatalogUtil
import weaver.*

import scala.jdk.CollectionConverters.*

object IcebergCatalogsSuite extends SimpleIOSuite:

  // Only proves that iceberg-aws and the shaded SDK in iceberg-aws-bundle load together; the
  // client needs a region to be built, but nothing is sent to AWS.
  test("S3FileIO from the aws bundle can be loaded through Iceberg's catalog properties") {
    val properties = Map("client.region" -> "eu-central-1").asJava
    IO.blocking(CatalogUtil.loadFileIO("org.apache.iceberg.aws.s3.S3FileIO", properties, null))
      .map(io => expect(io.newOutputFile("s3://bucket/key").location == "s3://bucket/key"))
  }

  private val warehouse: Resource[IO, os.Path] =
    Resource.make(IO.blocking(os.temp.dir(prefix = "iceberg-warehouse")))(dir =>
      IO.blocking(os.remove.all(dir))
    )

  test("a hadoop catalog built from config writes table metadata into the warehouse") {
    warehouse.use { dir =>
      val config = IcebergCatalogConfig(
        "local",
        Map("type" -> "hadoop", "warehouse" -> dir.toNIO.toUri.toString)
      )
      IcebergCatalogs.resource(config).use { catalog =>
        IO.blocking {
          catalog.createTable(Tables.users, IcebergSchemas.schemaFor[User])
          os.list(dir / "app" / "users" / "metadata").map(_.ext).toSet
        }.map(extensions => expect(extensions.contains("json")))
      }
    }
  }
