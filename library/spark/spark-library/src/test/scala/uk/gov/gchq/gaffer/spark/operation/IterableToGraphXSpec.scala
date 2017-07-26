/*
 * Copyright 2017 Crown Copyright
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

package uk.gov.gchq.gaffer.spark.operation

import uk.gov.gchq.gaffer.commonutil.spec.UnitSpec
import uk.gov.gchq.gaffer.data.element.{Edge, Element, Entity}
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser
import uk.gov.gchq.gaffer.spark.operation.graphx.IterableToGraphX

import scala.collection.JavaConverters._

class IterableToGraphXSpec extends UnitSpec {

  "The builder" should "create a populated operation" in {
    // Given
    val iter = createIterable()

    // When
    val op = new IterableToGraphX.Builder().input(iter).build

    val input = op.getInput
    val entities = input.asScala.filter(e => e.isInstanceOf[Entity])
    val edges = input.asScala.filter(e => e.isInstanceOf[Edge])

    // Then
    input shouldBe a[java.lang.Iterable[_]]
    entities should contain(new Entity.Builder().group("page").vertex("A").build())
    edges should contain(new Edge.Builder().directed(true).group("edge").source("A").dest("B").build())
  }

  "The operation" should "serialise and deserialise" in {
    // Given
    val op = new IterableToGraphX()
    val serialiser = new JSONSerialiser()

    // When
    val json = serialiser.serialise(op, true)
    val deserialisedOp = serialiser.deserialise(json, classOf[IterableToGraphX])

    // Then
    deserialisedOp shouldNot be (null)
  }

  def createIterable(): java.lang.Iterable[Element] = {
    val pageA = new Entity.Builder().group("page").vertex("A").build()
    val pageB = new Entity.Builder().group("page").vertex("B").build()
    val pageC = new Entity.Builder().group("page").vertex("C").build()
    val pageD = new Entity.Builder().group("page").vertex("D").build()

    val edge1 = new Edge.Builder().directed(true).group("edge").source("A").dest("B").build()
    val edge2 = new Edge.Builder().directed(true).group("edge").source("A").dest("C").build()
    val edge3 = new Edge.Builder().directed(true).group("edge").source("B").dest("C").build()
    val edge4 = new Edge.Builder().directed(true).group("edge").source("C").dest("A").build()
    val edge5 = new Edge.Builder().directed(true).group("edge").source("D").dest("C").build()

    List(pageA, pageB, pageC, pageD, edge1, edge2, edge3, edge4, edge5).asJava
  }

}
