package com.example.icebergflyway.model

import com.dtrosien.rowdata4s.annotations.{TableComment, TableName}

import java.time.Instant

/** Current shape of the `users` table as the apps see it. */
final case class User(
    @TableComment("primary key") id: Long,
    name: String,
    email: Option[String],
    @TableName("created_at") createdAt: Instant
)
