package flink.streaming.connectors.es.source

import java.util

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.configuration.{ConfigOption, ConfigOptions}
import org.apache.flink.table.connector.source.DynamicTableSource
import org.apache.flink.table.data.RowData
import org.apache.flink.table.factories.{DeserializationFormatFactory, DynamicTableFactory, DynamicTableSourceFactory, FactoryUtil}
import org.apache.flink.table.types.DataType

case class EsDynamicTableSourceFactory() extends DynamicTableSourceFactory {
  private val HOSTS: ConfigOption[String] = ConfigOptions.key("hosts")
    .stringType
    .noDefaultValue.
    withDescription("Elasticseatch hosts to connect to.")
  private val INDEX: ConfigOption[String] = ConfigOptions.key("index")
    .stringType
    .noDefaultValue
    .withDescription("Elasticsearch index for every record.")

  private val FORMAT: ConfigOption[String] = ConfigOptions.key("format")
    .stringType
    .noDefaultValue
    .withDescription("Elasticsearch doc format.")

  private val DOCUMENT_TYPE: ConfigOption[String] = ConfigOptions.key("document-type")
    .stringType
    .noDefaultValue
    .withDescription("Elasticsearch document type.")

  // look up config options
  private val LOOKUP_CACHE_MAX_ROWS = ConfigOptions.key("lookup.cache.max-rows")
    .longType
    .defaultValue(-1L)
    .withDescription("the max number of rows of lookup cache, over this value, the oldest rows will " + "be eliminated. \"cache.max-rows\" and \"cache.ttl\" options must all be specified if any of them is " + "specified. Cache is not enabled as default.")
  private val LOOKUP_CACHE_TTL = ConfigOptions.key("lookup.cache.ttl")
    .longType
    .defaultValue(10l)
    .withDescription("the cache time to live(second).")
  private val LOOKUP_MAX_RETRIES = ConfigOptions.key("lookup.max-retries")
    .intType
    .defaultValue(3)
    .withDescription("the max retry times if lookup database failed.")

  override def createDynamicTableSource(context: DynamicTableFactory.Context): DynamicTableSource = {
    val helper = FactoryUtil.createTableFactoryHelper(this, context)
    helper.validate()

    val decodingFormat = helper.discoverDecodingFormat[DeserializationSchema[RowData], DeserializationFormatFactory](
      classOf[DeserializationFormatFactory],
      FactoryUtil.FORMAT
    )

    val producedDataTypes: DataType = context.getCatalogTable.getSchema.toPhysicalRowDataType

    EsDynamicTableSource(
      decodingFormat,
      producedDataTypes,
      EsOptions(
        helper.getOptions.get(HOSTS),
        helper.getOptions.get(INDEX),
        helper.getOptions.get(DOCUMENT_TYPE)
      ),
      LookupOptions(
        helper.getOptions.get(LOOKUP_CACHE_MAX_ROWS).toLong,
        helper.getOptions.get(LOOKUP_CACHE_TTL).toLong,
        helper.getOptions.get(LOOKUP_MAX_RETRIES).toInt

      )
    )
  }

  override def factoryIdentifier(): String = "es"

  override def requiredOptions(): util.Set[ConfigOption[_]] = {
    val set = new util.HashSet[ConfigOption[_]]()
    set.add(HOSTS)
    set.add(FORMAT)
    set.add(INDEX)
    set.add(DOCUMENT_TYPE)
    set
  }

  override def optionalOptions(): util.Set[ConfigOption[_]] = {
    val set = new util.HashSet[ConfigOption[_]]()
    set.add(LOOKUP_CACHE_MAX_ROWS)
    set.add(LOOKUP_CACHE_TTL)
    set.add(LOOKUP_MAX_RETRIES)
    set
  }
}
