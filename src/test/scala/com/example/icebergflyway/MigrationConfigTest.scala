package com.example.icebergflyway

class MigrationConfigTest extends UnitSpec:

  "MigrationConfig" should "load the migration section of application.conf" in {
    val cfg       = MigrationConfig.load()
    val warehouse = s"file://${System.getProperty("user.dir")}/target/iceberg-warehouse"

    cfg.flyway.schema shouldBe "iceberg_flyway"
    cfg.iceberg.properties("type") shouldBe "hadoop"
    cfg.iceberg.properties("warehouse") shouldBe warehouse
    cfg.iceberg.properties.get("io-impl") shouldBe None
  }
