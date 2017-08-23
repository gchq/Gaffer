/*
 * Copyright 2017. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.utils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.ArrayListStringParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.BooleanParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.ByteParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.DateParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.DoubleParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.FloatParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.FreqMapParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.HashSetStringParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.InLineHyperLogLogPlusParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.IntegerParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.LongParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.ShortParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.StringParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.TreeSetStringParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.TypeSubTypeValueParquetSerialiser;
import uk.gov.gchq.gaffer.parquetstore.serialisation.impl.TypeValueParquetSerialiser;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.JavaSerialiser;

/**
 * A class to store all the constants regularly used throughout the Parquet store code.
 */
public final class ParquetStoreConstants {
    public static final String GRAPH = "graph";
    public static final String GROUP = "GROUP";
    public static final String SORTED = "sorted";
    public static final String INDEX = "_index";
    public static final String VERTEX = IdentifierType.VERTEX.name();
    public static final String SOURCE = IdentifierType.SOURCE.name();
    public static final String DESTINATION = IdentifierType.DESTINATION.name();
    public static final String DIRECTED = IdentifierType.DIRECTED.name();

    public static final String SPARK_SESSION_NAME = "Gaffer Parquet Store";

    @SuppressFBWarnings("MS_MUTABLE_ARRAY")
    public static final Serialiser[] SERIALISERS = new Serialiser[]{
            new StringParquetSerialiser(),
            new ByteParquetSerialiser(),
            new IntegerParquetSerialiser(),
            new LongParquetSerialiser(),
            new BooleanParquetSerialiser(),
            new DateParquetSerialiser(),
            new DoubleParquetSerialiser(),
            new FloatParquetSerialiser(),
            new InLineHyperLogLogPlusParquetSerialiser(),
            new ShortParquetSerialiser(),
            new TypeValueParquetSerialiser(),
            new FreqMapParquetSerialiser(),
            new TreeSetStringParquetSerialiser(),
            new TypeSubTypeValueParquetSerialiser(),
            new ArrayListStringParquetSerialiser(),
            new HashSetStringParquetSerialiser(),
            new JavaSerialiser()};

    private ParquetStoreConstants() {
    }
}
