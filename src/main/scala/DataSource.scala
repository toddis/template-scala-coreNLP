package org.template.classification

import java.util

import edu.stanford.nlp.process.Morphology
import io.prediction.controller.PDataSource
import io.prediction.controller.EmptyEvaluationInfo
import io.prediction.controller.EmptyActualResult
import io.prediction.controller.Params
import io.prediction.data.storage._

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import java.util.Scanner
import org.json4s.JsonAST.JValue
import org.template.classification.TrainingData

import scala.collection.mutable.{HashMap, HashSet}

import grizzled.slf4j.Logger

case class DataSourceParams(appId: Int) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
    EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {
    logger.error("made it here A")
    val eventsDb = Storage.getPEvents()
    var tweetCount = 0
    val dictSet = new HashSet[String]()
    val entityIdWordCountMap = new HashMap[Int, HashMap[String, Int]]
    val wordDocCount = new HashMap[String, Int]
    // Hack to iterate over data. How to do this properly?
    //    eventsDb.aggregateProperties(
    //      appId = dsp.appId,
    //      entityType = "user",
    //      // only keep entities with these required properties defined
    //      required = Some(List("sentiment", "tweet")))(sc)
    //      // aggregateProperties() returns RDD pair of
    //      // entity ID and its aggregated properties
    //      .map { case (entityId, properties) =>
    //      try {
    //        // tweet count hacky entityId
    //        stemAndTokenize(entityId.toInt, properties.get[String]("tweet"), dictSet, entityIdWordCountMap, wordDocCount)
    //        //        LabeledPoint(properties.get[Int]("attr0"),
    //        //          stemAndTokenize(properties.get[String]("tweet"))
    //        //        )
    //        tweetCount += 1
    //      } catch {
    //        case e: Exception => {
    //          logger.error(s"Failed to get properties ${properties} of" +
    //            s" ${entityId}. Exception: ${e}.")
    //          throw e
    //        }
    //      }
    //    }

    logger.error("Made it here B")
    val items: EntityMap[TweetSentiment] = eventsDb.extractEntityMap[TweetSentiment](
      appId = dsp.appId,
      entityType = "user",
      required = Some(Seq("sentiment", "tweet"))
    )(sc) { dm =>
      TweetSentiment(
        //sentiment = if (dm.get[Int]("sentiment") == 1) true else false,
        sentiment = dm.get[Int]("sentiment"),
        tweet = dm.get[String]("tweet")
      )
    }

    logger.error("dataMap size = " + items.size)

    //    val dict = dictSet.toArray
    //    dict.sortWith(_.compareTo(_) < 0)
    //
    //    val dictIndexes = getIndexMap(dict)
    //
    //    // Calculating Inverse Document Frequency
    //    val idfs = new Array[Double](dict.length)
    //    for (i <- 0 until dict.length) {
    //      idfs(i) = (tweetCount * 1.0) / wordDocCount(dict(i))
    //    }

    //    val labeledPoints: RDD[LabeledPoint] = eventsDb.aggregateProperties(
    //      appId = dsp.appId,
    //      entityType = "user",
    //      // only keep entities with these required properties defined
    //      required = Some(List("sentiment", "tweet")))(sc)
    //      // aggregateProperties() returns RDD pair of
    //      // entity ID and its aggregated properties
    //      .map { case (entityId, properties) =>
    //        try {
    //          // Get data array
    //          val data = new Array[Tuple2[Int, Double]](dict.length)
    //          val id = entityId.toInt
    //
    //          if (entityIdWordCountMap.contains(id)) {
    //            for ((word, count) <- entityIdWordCountMap(id)) {
    //              val index = dictIndexes(word)
    //              data(index) = new Tuple2(index, count * idfs(index))
    //            }
    //          }
    //          LabeledPoint(properties.get[Int]("sentiment"),
    //          Vectors.sparse(dict.length, data))
    //        } catch {
    //          case e: Exception => {
    //            logger.error(s"Failed to get properties ${properties} of" +
    //              s" ${entityId}. Exception: ${e}. THIS HAPPENED HERE")
    //            throw e
    //          }
    //        }
    //      }

    new TrainingData(null)
  }

  def stemAndTokenize(entityId: Int, tweet: String, dict: HashSet[String], entityIdWordCountMap: HashMap[Int, HashMap[String, Int]], wordDocCount: HashMap[String, Int]) {
    val scanner = new Scanner(tweet)
    //val stemmer = new Morphology()

    assert(!entityIdWordCountMap.contains(entityId))
    val wordCountMap = new HashMap[String, Int]
    entityIdWordCountMap.put(entityId, wordCountMap)

    val wordsInThisTweet = new HashSet[String]

    while (scanner.hasNext()) {
      val word = scanner.next() //(stemmer.stem(scanner.next()))
      wordsInThisTweet += word

      // wordCountMap
      if (!wordCountMap.contains(word)) {
        wordCountMap.put(word, 0)
      }
      wordCountMap.put(word, wordCountMap(word) + 1)
    }

    for (word <- wordsInThisTweet) {
      dict += word

      // word doc count
      if (!wordDocCount.contains(word)) {
        wordDocCount.put(word, 0)
      }
      wordDocCount.put(word, wordDocCount(word) + 1)
    }
  }

  def getIndexMap(dict: Array[String]) : HashMap[String, Int] = {
    val result = new HashMap[String, Int]
    for (i <- 0 until dict.length) {
      result.put(dict(i), i)
    }
    result
  }


}

case class TweetSentiment(
                           sentiment: Int,
                           tweet: String
                           )

class TrainingData(
                    val labeledPoints: RDD[LabeledPoint]
                    ) extends Serializable