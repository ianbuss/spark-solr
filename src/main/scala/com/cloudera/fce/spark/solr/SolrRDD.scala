package com.cloudera.fce.spark.solr

import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.common.SolrDocument
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}

import scala.reflect.ClassTag
import scala.collection.JavaConversions._
import scala.util.Random

case class SolrPartition(idx: Int, shardUrl: String, query: String, filterQuery: String = null) extends Partition {
  override def index: Int = idx
}

class SolrRDD[T: ClassTag](sc: SparkContext,
                           query: String,
                           collection: String,
                           config: Map[String, String],
                           transform: SolrDocument => T
                          ) extends RDD[T](sc, Nil) with Logging {

  override protected def getPartitions: Array[Partition] = {
    // - find out what shards are in a collection
    val server = SolrUtils.createSolrServer(config, collection)
    val shards = SolrUtils.getCollectionShards(server, collection)
    // - create a partition per shard for now

    val partitions = shards.zipWithIndex.map{ case (s, i) => {
      val replicaName = Random.shuffle(s.getReplicasMap.keySet.toList).head
      val replica = s.getReplicasMap.get(replicaName)
      SolrPartition(i, List(replica.get("base_url"), replica.get("core")).mkString("/"), query)
    } }
    // - find the min and max for a split column in each shard
    // - generate splits with queries with filter queries corresponding to splits

    partitions.toArray
  }


  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T] = new Iterator[T] {

    context.addTaskCompletionListener { context => closeIfNeeded() }

    val partition = split.asInstanceOf[SolrPartition]

    // Solr request state
    logInfo(s"Creating HttpSolrServer at ${partition.shardUrl}")
    val server = SolrUtils.createHttpSolrServer(config, partition.shardUrl)
    val query = new SolrQuery(partition.query)
    query.setDistrib(false)
    query.setRows(SolrRDD.DEFAULT_ROWS)
    logInfo(s"Executing query: ${query}")
    val response = server.query(query)

    val docs = response.getResults.iterator()

    // Iterator state
    private var closed = false

    // Iterator methods
    override def hasNext: Boolean = {
      docs.hasNext
    }

    override def next(): T = {
      val doc = docs.next()
      transform(doc)
    }

    def closeIfNeeded() = {
      if (!closed) {
        server.shutdown()
        closed = true
      }
    }

  }

}

object SolrRDD {

  val DEFAULT_ROWS = 100000

  implicit class SolrContext(val sc: SparkContext) {
    def solrRDD[T: ClassTag](query: String,
                   collection: String,
                   config: Map[String, String],
                   transform: SolrDocument => T) = {
      val conf = if (config.contains(SolrUtils.JAAS_PROPERTY) && !config(SolrUtils.JAAS_PROPERTY).isEmpty) {
        val server = SolrUtils.createSolrServer(config)
        config ++ Map(SolrUtils.DELEGATION_TOKEN_STRING -> SolrUtils.getDelegationToken(server))
      } else config
      new SolrRDD[T](sc, query, collection, conf, transform)
    }
  }

}
