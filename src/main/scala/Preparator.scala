package org.template.classification

import edu.stanford.nlp.ling.Word
import io.prediction.controller.PPreparator
import org.apache.lucene.analysis.en.EnglishAnalyzer

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import java.util.Scanner

import edu.stanford.nlp.process.Morphology
import edu.stanford.nlp.process.WhitespaceTokenizer.WhitespaceTokenizerFactory

import scala.collection.mutable

import java.util


class PreparedData(
  val labeledPoints: RDD[LabeledPoint]
) extends Serializable

class Preparator extends PPreparator[TrainingData, PreparedData] {

  def prepare(sc: SparkContext, trainingData: TrainingData): PreparedData = {


    var gatheredInfo = List.empty[Map[Int, Map[String, Int]]]

    for (point <- trainingData.labeledPoints) {

    }



    new PreparedData(trainingData.labeledPoints)
  }
}
