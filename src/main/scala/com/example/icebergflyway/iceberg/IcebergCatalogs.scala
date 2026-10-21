package com.example.icebergflyway.iceberg

import cats.effect.{IO, Resource}
import com.example.icebergflyway.IcebergCatalogConfig
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.CatalogUtil
import org.apache.iceberg.catalog.Catalog

import scala.jdk.CollectionConverters.*

object IcebergCatalogs:
  /** Builds the configured catalog and closes it on release (most catalogs are `Closeable`). */
  def resource(config: IcebergCatalogConfig): Resource[IO, Catalog] =
    Resource.make(
      IO.blocking(
        CatalogUtil.buildIcebergCatalog(config.name, config.properties.asJava, Configuration())
      )
    )(close)

  private def close(catalog: Catalog): IO[Unit] = catalog match
    case closeable: AutoCloseable => IO.blocking(closeable.close())
    case _                        => IO.unit
