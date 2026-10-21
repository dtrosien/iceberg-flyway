package com.example.icebergflyway.iceberg

import com.example.icebergflyway.IcebergCatalogConfig
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.CatalogUtil
import org.apache.iceberg.catalog.Catalog

import scala.jdk.CollectionConverters.*

object IcebergCatalogs:
  /** Builds the configured catalog, runs `use` and closes the catalog (most are `Closeable`). */
  def withCatalog[A](config: IcebergCatalogConfig)(use: Catalog => A): A =
    val catalog =
      CatalogUtil.buildIcebergCatalog(config.name, config.properties.asJava, Configuration())
    try use(catalog)
    finally
      catalog match
        case closeable: AutoCloseable => closeable.close()
        case _                        => ()
