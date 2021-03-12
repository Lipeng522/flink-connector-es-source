package flink.streaming.connectors.es.source

case class EsOptions(hosts: String, index: String, documentType: String)
