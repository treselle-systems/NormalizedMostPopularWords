package com.treselle.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up how many of each word appears in a book as simply as possible. */
object NormalizedMostPopularWords {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext 
    val conf = new SparkConf().setAppName("NormalizedMostPopularWords").set("spark.hadoop.validateOutputSpecs", "false")  
	val sc = new SparkContext(conf)
	
    // Read each line of the book into an RDD
    val lines = sc.textFile(args(0)) // RDD CREATED
    
	// Stop words
    val stopWords = sc.textFile(args(1))
    val stopWordSet = stopWords.collect.toSet
	
	// Distributed caching with broadcast variable
    val stopWordsBroadcast = sc.broadcast(stopWordSet)
	
    // Split using a regular expression that extracts words
    val words = lines.flatMap(x => x.split("\\W+").map(x => x.toLowerCase()).filter(x => x.matches("[A-Za-z]+") && x.length > 2).map(_.toLowerCase))
    
    // Normalize everything to lowercase
    val cleaned = words.mapPartitions{iterator => 
	  val stopWordsSet = stopWordsBroadcast.value
      iterator.filter(elem => !stopWordsSet.contains(elem))
    }
    
    // Count of the occurrences of each word
    val wordCounts = cleaned.map(x => (x, 1)).reduceByKey( (x,y) => x + y )
    
    // Flip (word, count) tuples to (count, word) and then sort by key (the counts)
    val wordCountsSorted = wordCounts.map( x => (x._2, x._1) ).sortByKey()

    // Save the file into HDFS.
    wordCountsSorted.saveAsTextFile(args(2))	
  }  
}

