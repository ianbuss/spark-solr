package com.cloudera.fce.spark.solr.example

import org.apache.solr.common.SolrDocument
import org.apache.spark.{SparkContext, SparkConf}

import com.cloudera.fce.spark.solr.SolrRDD._
import com.cloudera.fce.spark.solr.SolrUtils._

/**
  * Created by ianbuss on 9/27/16.
  */
object SolrRDDExample {

  def main(args: Array[String]) = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val docs = sc.solrRDD(
      args(0),
      args(1),
      Map(
        ZK_CONNECTION_STRING_PARAM -> args(2),
        JAAS_PROPERTY -> args(3)),
      (d: SolrDocument) => d.toString
    )

    val nums = docs.mapPartitions(it => {
      List(it.map(x => 1).sum).iterator
    })
    nums.collect.foreach(println)

  }

}
