/*
 * Copyright 2016 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.gov.gchq.gaffer.spark.examples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import uk.gov.gchq.gaffer.data.elementdefinition.view.View
import uk.gov.gchq.gaffer.doc.operation.OperationExample
import uk.gov.gchq.gaffer.graph.Graph
import uk.gov.gchq.gaffer.operation.OperationException
import uk.gov.gchq.gaffer.operation.data.EdgeSeed
import uk.gov.gchq.gaffer.spark.SparkConstants
import uk.gov.gchq.gaffer.spark.operation.scalardd.GetRDDOfElements
import uk.gov.gchq.gaffer.user.User

/**
 * An example showing how the {@link GetRDDOfElements} operation is used from Scala.
 */
class GetRDDOfElementsExample() extends OperationExample(classOf[GetRDDOfElements]) {
  private lazy val ROOT_LOGGER = Logger.getRootLogger

  override def runExamples() {
    // Need to actively turn logging on and off as needed as Spark produces some logs
    // even when the log level is set to off.
    ROOT_LOGGER.setLevel(Level.OFF)
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("getDataFrameOfElementsWithEntityGroup")
      .set(SparkConstants.SERIALIZER, SparkConstants.DEFAULT_SERIALIZER)
      .set(SparkConstants.KRYO_REGISTRATOR, SparkConstants.DEFAULT_KRYO_REGISTRATOR)
      .set(SparkConstants.DRIVER_ALLOW_MULTIPLE_CONTEXTS, "true")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    sparkSession.sparkContext.setLogLevel("OFF")
    val graph = getGraph
    try {
      getRddOfElements(sparkSession, graph)
      getRddOfElementsReturningEdgesOnly(sparkSession, graph)
    } catch {
      case e: OperationException => {
        sparkSession.stop()
        throw new RuntimeException(e)
      }
    }
    sparkSession.stop()
    ROOT_LOGGER.setLevel(Level.INFO)
  }

  @throws[OperationException]
  def getRddOfElements(sparkSession: SparkSession, graph: Graph) {
    ROOT_LOGGER.setLevel(Level.INFO)
    // Avoid using getMethodNameAsSentence as it messes up the formatting of the "RDD" part
    log("#### get RDD of elements\n")
    printGraph()
    ROOT_LOGGER.setLevel(Level.OFF)
    val operation = new GetRDDOfElements.Builder()
      .input(new EdgeSeed(1, 2, true), new EdgeSeed(2, 3, true))
      .sparkSession(sparkSession)
      .build
    val rdd = graph.execute(operation, new User("user01"))
    val elements = rdd.collect()
    ROOT_LOGGER.setLevel(Level.INFO)
    printScala(
      """val operation = new GetRDDOfElements.Builder()
        |    .input(new EdgeSeed(1, 2, true), new EdgeSeed(2, 3, true))
        |    .sparkContext(sc)
        |    .build()
        |val rdd = graph.execute(operation, new User(\"user01\"))
        |val elements = rdd.collect())""".stripMargin)
    log("The results are:\n")
    log("```")
    for (e <- elements) {
      log(e.toString)
    }
    log("```")
    log("The normal Spark methods are now available on the RDD, e.g.:")
    printScala("rdd.count()")
    log("The result is:")
    log("```")
    ROOT_LOGGER.setLevel(Level.OFF)
    val count = rdd.count()
    ROOT_LOGGER.setLevel(Level.INFO)
    log(s"$count")
    log("```")
    ROOT_LOGGER.setLevel(Level.OFF)
  }

  @throws[OperationException]
  def getRddOfElementsReturningEdgesOnly(sparkSession: SparkSession, graph: Graph) {
    ROOT_LOGGER.setLevel(Level.INFO)
    log("#### get RDD of elements returning edges only\n")
    printGraph()
    ROOT_LOGGER.setLevel(Level.OFF)
    val operation = new GetRDDOfElements.Builder()
      .input(new EdgeSeed(1, 2, true), new EdgeSeed(2, 3, true))
      .view(new View.Builder()
        .edge("edge")
        .build)
      .sparkSession(sparkSession)
      .build
    val rdd = graph.execute(operation, new User("user01"))
    val elements = rdd.collect()
    ROOT_LOGGER.setLevel(Level.INFO)
    printScala(
      """val operation = new GetRDDOfElements.Builder()
        |    .input(new EdgeSeed(1, 2, true), new EdgeSeed(2, 3, true))
        |    .view(new View.Builder()
        |                .edge("edge")
        |                .build())
        |    .sparkContext(sc)
        |    .build()
        |val rdd = graph.execute(operation, new User(\"user01\"))
        |val elements = rdd.collect())
      """.stripMargin)
    log("The results are:\n")
    log("```")
    for (e <- elements) {
      log(e.toString)
    }
    log("```")
    ROOT_LOGGER.setLevel(Level.OFF)
  }
}

object GetRDDOfElementsExample {
  @throws[OperationException]
  def main(args: Array[String]) {
    new GetRDDOfElementsExample().runExamples()
  }
}