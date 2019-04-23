/*
 * Copyright 2018-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.store;

import uk.gov.gchq.gaffer.serialisation.FreqMapSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.ordered.OrderedDateSerialiser;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.gaffer.types.function.FreqMapAggregator;
import uk.gov.gchq.koryphe.impl.binaryoperator.Min;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringDeduplicateConcat;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;

import java.util.Date;

public final class TestTypes {
    // Type Names
    public static final String TIMESTAMP = "timestamp";
    public static final String TIMESTAMP_2 = "timestamp2";
    public static final String VISIBILITY = "visibility";
    public static final String ID_STRING = "id.string";
    public static final String DIRECTED_EITHER = "directed.either";
    public static final String DIRECTED_TRUE = "directed.true";
    public static final String PROP_STRING = "prop.string";
    public static final String PROP_INTEGER = "prop.integer";
    public static final String PROP_INTEGER_2 = "prop.integer.2";
    public static final String PROP_LONG = "prop.long";
    public static final String PROP_COUNT = "prop.count";
    public static final String PROP_MAP = "prop.map";
    public static final String PROP_SET_STRING = "prop.set.string";
    public static final String VERTEX_STRING = "vertex.string";
    public static final String PROP_DATE = "prop.date";

    private TestTypes() {
        // private constructor to prevent instantiation
    }

    // Type implementations
    public static final TypeDefinition STRING_TYPE = new TypeDefinition.Builder()
            .clazz(String.class)
            .aggregateFunction(new StringConcat())
            .build();

    public static final TypeDefinition INTEGER_TYPE = new TypeDefinition.Builder()
            .clazz(Integer.class)
            .aggregateFunction(new Sum())
            .build();

    public static final TypeDefinition LONG_TYPE = new TypeDefinition.Builder()
            .clazz(Long.class)
            .aggregateFunction(new Sum())
            .build();

    public static final TypeDefinition BOOLEAN_TYPE = new TypeDefinition.Builder()
            .clazz(Boolean.class)
            .build();

    public static final TypeDefinition FREQMAP_TYPE = new TypeDefinition.Builder()
            .clazz(FreqMap.class)
            .serialiser(new FreqMapSerialiser())
            .aggregateFunction(new FreqMapAggregator())
            .build();

    public static final TypeDefinition SET_STRING_TYPE = new TypeDefinition.Builder()
            .clazz(String.class)
            .aggregateFunction(new StringDeduplicateConcat())
            .build();

    public static final TypeDefinition DATE_TYPE = new TypeDefinition.Builder()
            .clazz(Date.class)
            .serialiser(new OrderedDateSerialiser())
            .aggregateFunction(new Min())
            .build();
}
