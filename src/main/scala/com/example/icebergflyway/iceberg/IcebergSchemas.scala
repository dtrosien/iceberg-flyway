package com.example.icebergflyway.iceberg

import com.dtrosien.rowdata4s.datatype.{DataTypeFor, FlinkDataType}
import org.apache.flink.table.types.logical.RowType
import org.apache.iceberg.Schema
import org.apache.iceberg.flink.FlinkSchemaUtil
import org.apache.iceberg.types.TypeUtil

import java.util.concurrent.atomic.AtomicInteger
import scala.jdk.CollectionConverters.*

/** Derives Iceberg schemas from case classes: rowdata4s turns the case class into a Flink
  * `DataType`, iceberg-flink turns that into Iceberg types. Field ids in the result are
  * placeholders; Iceberg assigns fresh ids on `createTable` and matches by name on evolution.
  */
object IcebergSchemas:

  def rowType[T: DataTypeFor]: RowType =
    FlinkDataType[T].getLogicalType.asInstanceOf[RowType]

  def schemaFor[T: DataTypeFor]: Schema =
    Schema(FlinkSchemaUtil.convert(rowType[T]).asStructType.fields)

  /** Id-free view of a schema's top-level columns, for comparing a table with a case class. */
  final case class Column(name: String, `type`: String, optional: Boolean)

  def columns(schema: Schema): List[Column] =
    schema.columns.asScala.toList.map { field =>
      // Struct types render nested field ids, so number them from 1 inside every column.
      val next = AtomicInteger(0)
      val tpe  = TypeUtil.assignFreshIds(field.`type`, () => next.incrementAndGet())
      Column(field.name, tpe.toString, field.isOptional)
    }
