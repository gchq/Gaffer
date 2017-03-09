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
import org.apache.spark.{SparkConf, SparkContext}
import uk.gov.gchq.gaffer.example.operation.OperationExample
import uk.gov.gchq.gaffer.graph.Graph
import uk.gov.gchq.gaffer.operation.OperationException
import uk.gov.gchq.gaffer.spark.operation.scalardd.GetRDDOfAllElements
import uk.gov.gchq.gaffer.user.User

/**
  * An example showing how the {@link GetJavaRDDOfElements} operation is used from Scala.
  */
class GetRDDOfAllElementsExample extends OperationExample(classOf[GetRDDOfAllElements]) {
  private lazy val ROOT_LOGGER = Logger.getRootLogger

  override def runExamples() {
    // Need to actively turn logging on and off as needed as Spark produces some logs
    // even when the log level is set to off.
    ROOT_LOGGER.setLevel(Level.OFF)
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("GetRDDOfAllElementsExample")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "uk.gov.gchq.gaffer.spark.serialisation.kryo.Registrator")
      .set("spark.driver.allowMultipleContexts", "true")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("OFF")
    try {
      getRddOfAllElements(sc, getGraph)
    } catch {
      case e: OperationException => {
        sc.stop()
        throw new RuntimeException(e)
      }
    }
    sc.stop()
    ROOT_LOGGER.setLevel(Level.INFO)
  }

  @throws[OperationException]
  def getRddOfAllElements(sc: SparkContext, graph: Graph) {
    ROOT_LOGGER.setLevel(Level.INFO)
    // Avoid using getMethodNameAsSentence as it messes up the formatting of the "RDD" part
    log("#### get RDD of all elements\n")
    printGraph()
    ROOT_LOGGER.setLevel(Level.OFF)
    val operation = new GetRDDOfAllElements.Builder()
      .sparkContext(sc)
      .build
    val rdd = graph.execute(operation, new User("user01"))
    val elements = rdd.collect
    ROOT_LOGGER.setLevel(Level.INFO)
    printScala("""val operation = new GetRDDOfAllElements.Builder()
                 |    .sparkContext(sc)
                 |    .build()
                 |val rdd = graph.execute(operation, new User(\"user01\"))
                 |val elements = rdd.collect())""".stripMargin)
    log("The results are:")
    log("```")
    for (e <- elements) {
      log(e.toString)
    }
    log("```")
    ROOT_LOGGER.setLevel(Level.OFF)
  }
}

object GetRDDOfAllElementsExample {
  @throws[OperationException]
  def main(args: Array[String]) {
    new GetRDDOfAllElementsExample().run()
  }
}