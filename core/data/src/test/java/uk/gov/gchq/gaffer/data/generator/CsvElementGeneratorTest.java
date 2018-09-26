/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.data.generator;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.element.function.PropertiesTransformer;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.impl.function.SetValue;
import uk.gov.gchq.koryphe.impl.function.ToLong;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class CsvElementGeneratorTest {

    @Test
    public void shouldJsonSerialise() throws SerialisationException {
        // Given
        final CsvElementGenerator generator = new CsvElementGenerator()
                .header("src", "dest", "time")
                .transformer(new PropertiesTransformer.Builder()
                        .select("time").execute(new ToLong()).project("time")
                        .build())
                .edge("Edge", new ElementTransformer.Builder()
                        .select("src").project("SOURCE")
                        .select("dest").project("DESTINATION")
                        .select("time").project("timestamps")
                        .select("time").execute(new CommonTimeUtil.ToTimeBucket(CommonTimeUtil.TimeBucket.HOUR)).project("timebucket")
                        .select().execute(new SetValue(1)).project("count")
                        .build());
        // When
        final byte[] json = JSONSerialiser.serialise(generator, true);
        final CsvElementGenerator deserialised = JSONSerialiser.deserialise(json, CsvElementGenerator.class);

        // Then
        assertEquals(generator.getHeader(), deserialised.getHeader());
    }

    @Test
    public void test() {
        // Given
        final CsvElementGenerator generator = new CsvElementGenerator()
                .header("src", "dest", "time")
                .transformer(new PropertiesTransformer.Builder()
                        .select("time").execute(new ToLong()).project("time")
                        .build())
                .edge("Edge", new ElementTransformer.Builder()
                        .select("src").project("SOURCE")
                        .select("dest").project("DESTINATION")
                        .select("time").project("timestamps")
                        .select("time").execute(new CommonTimeUtil.ToTimeBucket(CommonTimeUtil.TimeBucket.HOUR)).project("timebucket")
                        .select().execute(new SetValue(1)).project("count")
                        .build());

        // When
        final Iterable<? extends Element> elements = generator.apply(Arrays.asList(
                "1,4,1254192988",
                "1,4,1254192989",
                "1,4,1254192990"
        ));

        // Then
        ElementUtil.assertElementEquals(Arrays.asList(
                new Edge.Builder()
                        .group("Edge")
                        .source("1")
                        .dest("4")
                        .property("count", 1)
                        .property("timestamps", 1254192988L)
                        .property("timebucket", 1252800000L)
                        .build(),
                new Edge.Builder()
                        .group("Edge")
                        .source("1")
                        .dest("4")
                        .property("count", 1)
                        .property("timestamps", 1254192989L)
                        .property("timebucket", 1252800000L)
                        .build(),
                new Edge.Builder()
                        .group("Edge")
                        .source("1")
                        .dest("4")
                        .property("count", 1)
                        .property("timestamps", 1254192990L)
                        .property("timebucket", 1252800000L)
                        .build()
        ), elements);
    }
}
