package flink.streaming.connectors.es.source

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.shaded.guava18.com.google.common.cache.{Cache, CacheBuilder}
import org.apache.flink.table.data.{GenericRowData, RowData, StringData}
import org.apache.flink.table.functions.{FunctionContext, TableFunction}
import org.apache.http.HttpHost
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.slf4j.{Logger, LoggerFactory}

case class EsDynamicTableSourceFunction(esOptions: EsOptions,
                                        lookupOptions: LookupOptions,
                                        deserializer: DeserializationSchema[RowData]) extends TableFunction[RowData] {
  private val logger: Logger = LoggerFactory.getLogger(classOf[EsDynamicTableSourceFunction])
  private var cache: Cache[RowData, Seq[RowData]] = _

  private var client: RestHighLevelClient = _

  override def open(context: FunctionContext): Unit = {
    logger.info(s"options:$esOptions: lookupOptions:$lookupOptions")
    client = new RestHighLevelClient(RestClient.builder(esOptions.hosts.split(",").map(HttpHost.create): _*))
    this.cache = if (lookupOptions.maxRows == -1 || lookupOptions.ttl == -1) null
    else CacheBuilder.newBuilder
      .expireAfterWrite(lookupOptions.ttl, TimeUnit.MILLISECONDS)
      .maximumSize(lookupOptions.maxRows)
      .build[RowData, Seq[RowData]]
    logger.info("es client opened")
  }

  def eval(idData: StringData): Unit = {
    val key = GenericRowData.of(idData)
    if (cache != null) {
      val rows = cache.getIfPresent(key)
      if (rows != null) {
        rows.foreach(collect)
        return
      }
    }
    for (retry <- 1 to lookupOptions.retry) {
      try {
        val id = new String(idData.toBytes)
        val response = client.get(new GetRequest(esOptions.index, esOptions.documentType, id), RequestOptions.DEFAULT)
        if (response.isExists && !response.isSourceEmpty) {
          val data = deserializer.deserialize(response.getSourceAsBytes)
          if (cache != null) {
            cache.put(key, Seq(data))
          }
          collect(data)
        }
        return
      } catch {
        case exception: Exception =>
          if (retry >= lookupOptions.retry) {
            throw new RuntimeException("insert error", exception)
          }
          logger.error(exception.getMessage, exception)
          Thread.sleep(1000 * retry)

      }
    }
  }

  override def close(): Unit = {
    if (cache != null) {
      cache.cleanUp()
    }
    client.close()
  }
}
