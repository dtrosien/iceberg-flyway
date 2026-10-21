package com.example.icebergflyway.migrations

import com.dtrosien.rowdata4s.annotations.{TableComment, TableName}
import com.example.icebergflyway.iceberg.IcebergSchemas
import com.example.icebergflyway.migration.IcebergMigration
import com.example.icebergflyway.model.Tables
import org.apache.iceberg.catalog.Catalog

import java.time.Instant

/** `users` as it looked when this migration was written. Later changes live in later migrations, so
  * the model classes can move on without rewriting history.
  */
private final case class UserV1(
    @TableComment("primary key") id: Long,
    name: String,
    @TableName("created_at") createdAt: Instant
)

final class V1__Create_users_table(catalog: Catalog) extends IcebergMigration(catalog):
  def run(): Unit =
    createTable(Tables.users, IcebergSchemas.schemaFor[UserV1], partitioning = _.day("created_at"))
