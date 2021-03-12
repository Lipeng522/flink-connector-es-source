package flink.streaming.connectors.es.source

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.table.connector.format.DecodingFormat
import org.apache.flink.table.connector.source.{DynamicTableSource, LookupTableSource, TableFunctionProvider}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.types.DataType

case class EsDynamicTableSource(decodingFormat: DecodingFormat[DeserializationSchema[RowData]],
                                producedDataTypes: DataType,
                                options: EsOptions,
                                lookupOptions: LookupOptions
                               ) extends LookupTableSource {
  override def copy(): DynamicTableSource = EsDynamicTableSource(decodingFormat, producedDataTypes, options, lookupOptions)

  override def asSummaryString(): String = "es table source"

  override def getLookupRuntimeProvider(context: LookupTableSource.LookupContext): LookupTableSource.LookupRuntimeProvider = {
    val deserializer: DeserializationSchema[RowData] = decodingFormat.createRuntimeDecoder(context, producedDataTypes)
    TableFunctionProvider.of(EsDynamicTableSourceFunction(options, lookupOptions, deserializer))
  }
}
