package flink.streaming.connectors

import org.apache.flink.table.types.logical.LogicalType

case class CatalogTableColumnInfo(name: String, logicalType: LogicalType)
