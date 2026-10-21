package com.example.icebergflyway

import weaver.*

object MigrationConfigSuite extends SimpleIOSuite:

  test("the migration section of application.conf is loaded into MigrationConfig") {
    MigrationConfig.load.map { cfg =>
      val warehouse = s"file://${System.getProperty("user.dir")}/target/iceberg-warehouse"
      expect(cfg.flyway.schema == "iceberg_flyway") and
        expect(cfg.iceberg.properties("type") == "hadoop") and
        expect(cfg.iceberg.properties("warehouse") == warehouse) and
        expect(cfg.iceberg.properties.get("io-impl").isEmpty)
    }
  }
