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

    logger.info("Made it here B")
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

    logger.info("Made it here C")

    new TrainingData(null)
  }
}

case class TweetSentiment(
   sentiment: Int,
   tweet: String
   )

class TrainingData(
  val labeledPoints: RDD[LabeledPoint]
  ) extends Serializable