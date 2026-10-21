package com.example.icebergflyway

import com.example.icebergflyway.migration.MigrateApp
import com.example.icebergflyway.migrations.Migrations
import com.example.icebergflyway.model.Tables

/** The main class of this migration set; a multi-project repo has one of these per set. */
object MigrateMain extends MigrateApp(Migrations.all, Tables.all)
