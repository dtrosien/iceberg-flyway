package com.example.icebergflyway.iceberg

import com.example.icebergflyway.iceberg.IcebergSchemas.Column
import com.example.icebergflyway.model.User
import weaver.*

object IcebergSchemasSuite extends SimpleIOSuite:

  pureTest("User derives the expected Iceberg columns") {
    val expected = List(
      Column("id", "long", optional = false),
      Column("name", "string", optional = false),
      Column("email", "string", optional = true),
      Column("created_at", "timestamptz", optional = false)
    )
    expect.same(expected, IcebergSchemas.columns(IcebergSchemas.schemaFor[User]))
  }

  pureTest("@TableComment becomes the column doc") {
    expect(IcebergSchemas.schemaFor[User].findField("id").doc == "primary key")
  }
