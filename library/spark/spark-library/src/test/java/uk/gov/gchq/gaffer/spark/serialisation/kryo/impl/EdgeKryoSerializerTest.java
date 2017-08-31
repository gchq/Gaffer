/*
 * Copyright 2016-2017 Crown Copyright
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
package uk.gov.gchq.gaffer.spark.serialisation.kryo.impl;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.KryoSerializerTest;

public class EdgeKryoSerializerTest extends KryoSerializerTest<Edge> {

    public Class<Edge> getTestClass() {
        return Edge.class;
    }

    public Edge getTestObject() {
        return new Edge.Builder()
                .group("group")
                .source("abc")
                .dest("xyz")
                .directed(true)
                .property("property1", 1)
                .build();
    }
}
