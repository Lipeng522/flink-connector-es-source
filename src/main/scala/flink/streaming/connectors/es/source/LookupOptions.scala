package flink.streaming.connectors.es.source

case class LookupOptions(maxRows: Long, ttl: Long, retry: Int)
