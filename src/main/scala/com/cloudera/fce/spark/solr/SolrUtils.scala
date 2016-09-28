package com.cloudera.fce.spark.solr

import java.io.IOException

import org.apache.solr.client.solrj.SolrServer
import org.apache.solr.client.solrj.impl.{CloudSolrServer, HttpSolrServer}
import org.apache.solr.client.solrj.request.DelegationTokenRequest
import org.apache.solr.common.cloud.Slice

import scala.collection.JavaConversions._

object SolrUtils {

  val ZK_CONNECTION_STRING_PARAM = "solr.zookeeper.quorum"
  val DELEGATION_TOKEN_STRING = "solr.delegation.token"
  val JAAS_PROPERTY = "java.security.auth.login.config"

  private var solrServer: CloudSolrServer = _

  def createSolrServer(config: Map[String, String], collection: String = ""): CloudSolrServer = {
    if (solrServer == null) {
      if (config.contains(DELEGATION_TOKEN_STRING)) {
        System.setProperty(HttpSolrServer.DELEGATION_TOKEN_PROPERTY, config(DELEGATION_TOKEN_STRING))
      }
      solrServer = new CloudSolrServer(config(ZK_CONNECTION_STRING_PARAM))
      if (!collection.isEmpty) {
        solrServer.setDefaultCollection(collection)
      }
    }

    solrServer
  }

  def createHttpSolrServer(config: Map[String, String], url: String): HttpSolrServer = {
    if (config.contains(DELEGATION_TOKEN_STRING)) {
      System.setProperty(HttpSolrServer.DELEGATION_TOKEN_PROPERTY, config(DELEGATION_TOKEN_STRING))
    }
    new HttpSolrServer(url)
  }

  def getDelegationToken(jaasConfigFile: String, solrServer: SolrServer): String = {
    System.setProperty(JAAS_PROPERTY, jaasConfigFile)
    val request = new DelegationTokenRequest.Get
    val response = request.process(solrServer)
    response.getDelegationToken
  }

  def getCollectionShards(solrServer: CloudSolrServer, collection: String): List[Slice] = {
    val ping = solrServer.ping()
    if (ping.getStatus != 0) {
      throw new IOException(s"Could not ping SolrServer: ${solrServer}")
    }
    val zkReader = solrServer.getZkStateReader
    val state = zkReader.getClusterState
    val collectionInfo = state.getCollection(collection)
    collectionInfo.getActiveSlices.toList
  }

}
