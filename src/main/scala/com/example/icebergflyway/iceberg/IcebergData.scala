package com.example.icebergflyway.iceberg

import org.apache.iceberg.{DataFile, DataFiles, FileFormat, PartitionKey, Schema, Table}
import org.apache.iceberg.data.{InternalRecordWrapper, Record}
import org.apache.iceberg.data.parquet.GenericParquetWriter
import org.apache.iceberg.parquet.Parquet
import org.apache.parquet.schema.MessageType

import java.util.UUID
import scala.util.Using

object IcebergData:
  /** Writes the rows into one Parquet file per partition and appends them in a single commit. */
  def append(table: Table, rows: Iterable[Record]): Unit =
    val wrapper     = InternalRecordWrapper(table.schema.asStruct)
    val byPartition = rows.toList.groupBy { row =>
      val key = PartitionKey(table.spec, table.schema)
      key.partition(wrapper.wrap(row))
      key
    }
    val append = table.newAppend()
    byPartition.foreach((partition, group) => append.appendFile(writeFile(table, partition, group)))
    append.commit()

  private def writeFile(table: Table, partition: PartitionKey, rows: List[Record]): DataFile =
    val fileName = FileFormat.PARQUET.addExtension(UUID.randomUUID().toString)
    val location =
      if table.spec.isUnpartitioned then table.locationProvider.newDataLocation(fileName)
      else table.locationProvider.newDataLocation(table.spec, partition, fileName)
    val file     = table.io.newOutputFile(location)
    val appender = Parquet
      .write(file)
      .forTable(table)
      .createWriterFunc((schema: Schema, tpe: MessageType) =>
        GenericParquetWriter.create(schema, tpe)
      )
      .build[Record]()
    Using.resource(appender)(a => rows.foreach(a.add))
    val dataFile = DataFiles
      .builder(table.spec)
      .withInputFile(file.toInputFile)
      .withFormat(FileFormat.PARQUET)
      .withMetrics(appender.metrics)
    if table.spec.isPartitioned then dataFile.withPartition(partition)
    dataFile.build()
