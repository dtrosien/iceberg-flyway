package com.example.icebergflyway.iceberg

import com.example.icebergflyway.{IcebergCatalogConfig, UnitSpec}
import com.example.icebergflyway.model.{Tables, User}
import org.apache.iceberg.CatalogUtil

import scala.jdk.CollectionConverters.*

class IcebergCatalogsTest extends UnitSpec:

  // Only proves that iceberg-aws and the shaded SDK in iceberg-aws-bundle load together; the
  // client needs a region to be built, but nothing is sent to AWS.
  "S3FileIO" should "load from the aws bundle through Iceberg's catalog properties" in {
    val properties = Map("client.region" -> "eu-central-1").asJava
    val io         = CatalogUtil.loadFileIO("org.apache.iceberg.aws.s3.S3FileIO", properties, null)

    io.newOutputFile("s3://bucket/key").location shouldBe "s3://bucket/key"
  }

  "A hadoop catalog built from config" should "write table metadata into the warehouse" in {
    val dir = os.temp.dir(prefix = "iceberg-warehouse")
    try
      val config = IcebergCatalogConfig(
        "local",
        Map("type" -> "hadoop", "warehouse" -> dir.toNIO.toUri.toString)
      )
      val extensions = IcebergCatalogs.withCatalog(config) { catalog =>
        catalog.createTable(Tables.users, IcebergSchemas.schemaFor[User])
        os.list(dir / "app" / "users" / "metadata").map(_.ext).toSet
      }
      extensions should contain("json")
    finally os.remove.all(dir)
  }
