package com.example.icebergflyway.iceberg

import com.example.icebergflyway.UnitSpec
import com.example.icebergflyway.iceberg.IcebergSchemas.Column
import com.example.icebergflyway.model.User

class IcebergSchemasTest extends UnitSpec:

  "IcebergSchemas" should "derive the expected Iceberg columns for User" in {
    IcebergSchemas.columns(IcebergSchemas.schemaFor[User]) shouldBe List(
      Column("id", "long", optional = false),
      Column("name", "string", optional = false),
      Column("email", "string", optional = true),
      Column("created_at", "timestamptz", optional = false)
    )
  }

  it should "turn @TableComment into the column doc" in {
    IcebergSchemas.schemaFor[User].findField("id").doc shouldBe "primary key"
  }
