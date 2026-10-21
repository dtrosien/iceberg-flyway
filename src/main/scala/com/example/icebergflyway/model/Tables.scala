package com.example.icebergflyway.model

import org.apache.iceberg.catalog.{Namespace, TableIdentifier}

object Tables:
  val namespace: Namespace       = Namespace.of("app")
  val users: TableIdentifier     = TableIdentifier.of(namespace, "users")
  val countries: TableIdentifier = TableIdentifier.of(namespace, "countries")

  /** Every table the migrations manage; checked for outside changes before migrating. */
  val all: List[TableIdentifier] = List(users, countries)
