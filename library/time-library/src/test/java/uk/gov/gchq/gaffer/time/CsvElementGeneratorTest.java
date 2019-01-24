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

package uk.gov.gchq.gaffer.time;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.gaffer.commonutil.function.ToTimeBucketStart;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.PropertiesTransformer;
import uk.gov.gchq.gaffer.data.generator.CsvElementDef;
import uk.gov.gchq.gaffer.data.generator.CsvElementGenerator;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.koryphe.impl.function.SetValue;
import uk.gov.gchq.koryphe.impl.function.ToLong;

import java.time.Instant;
import java.util.Arrays;

public class CsvElementGeneratorTest {
    @Test
    public void shouldGenerateElementsFromAGivenCsv() {
        // Given
        final CsvElementGenerator generator = new CsvElementGenerator()
                .header("src", "dest", "time")
                .transformer(new PropertiesTransformer.Builder()
                        .select("time").execute(new ToLong()).project("time")
                        .select("time").execute(new ToTimestampSet(CommonTimeUtil.TimeBucket.HOUR, true)).project("timestamps")
                        .select("time").execute(new ToTimeBucketStart(CommonTimeUtil.TimeBucket.HOUR)).project("timebucket")
                        .select().execute(new SetValue(1)).project("count")
                        .build())
                .element(new CsvElementDef("Edge")
                        .source("src")
                        .destination("dest")
                        .directed(false)
                        .property("timestamps")
                        .property("timebucket")
                        .property("count"))
                .element(new CsvElementDef("Entity")
                        .vertex("src")
                        .property("timestamps")
                        .property("timebucket")
                        .property("count"))
                .element(new CsvElementDef("Entity")
                        .vertex("dest")
                        .property("timestamps")
                        .property("timebucket")
                        .property("count"));


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
                        .property("timestamps", new RBMBackedTimestampSet.Builder()
                                .timeBucket(CommonTimeUtil.TimeBucket.HOUR)
                                .timestamp(Instant.ofEpochMilli(1254192988000L))
                                .build())
                        .property("timebucket", 1252800000L)
                        .build(),
                new Edge.Builder()
                        .group("Edge")
                        .source("1")
                        .dest("4")
                        .property("count", 1)
                        .property("timestamps", new RBMBackedTimestampSet.Builder()
                                .timeBucket(CommonTimeUtil.TimeBucket.HOUR)
                                .timestamp(Instant.ofEpochMilli(1254192989000L))
                                .build())
                        .property("timebucket", 1252800000L)
                        .build(),
                new Edge.Builder()
                        .group("Edge")
                        .source("1")
                        .dest("4")
                        .property("count", 1)
                        .property("timestamps", new RBMBackedTimestampSet.Builder()
                                .timeBucket(CommonTimeUtil.TimeBucket.HOUR)
                                .timestamp(Instant.ofEpochMilli(1254192990000L))
                                .build())
                        .property("timebucket", 1252800000L)
                        .build(),
                new Entity.Builder()
                        .group("Entity")
                        .vertex("1")
                        .property("count", 1)
                        .property("timestamps", new RBMBackedTimestampSet.Builder()
                                .timeBucket(CommonTimeUtil.TimeBucket.HOUR)
                                .timestamp(Instant.ofEpochMilli(1254192990000L))
                                .build())
                        .property("timebucket", 1252800000L)
                        .build(),
                new Entity.Builder()
                        .group("Entity")
                        .vertex("4")
                        .property("count", 1)
                        .property("timestamps", new RBMBackedTimestampSet.Builder()
                                .timeBucket(CommonTimeUtil.TimeBucket.HOUR)
                                .timestamp(Instant.ofEpochMilli(1254192990000L))
                                .build())
                        .property("timebucket", 1252800000L)
                        .build(),
                new Entity.Builder()
                        .group("Entity")
                        .vertex("1")
                        .property("count", 1)
                        .property("timestamps", new RBMBackedTimestampSet.Builder()
                                .timeBucket(CommonTimeUtil.TimeBucket.HOUR)
                                .timestamp(Instant.ofEpochMilli(1254192990000L))
                                .build())
                        .property("timebucket", 1252800000L)
                        .build(),
                new Entity.Builder()
                        .group("Entity")
                        .vertex("4")
                        .property("count", 1)
                        .property("timestamps", new RBMBackedTimestampSet.Builder()
                                .timeBucket(CommonTimeUtil.TimeBucket.HOUR)
                                .timestamp(Instant.ofEpochMilli(1254192988000L))
                                .build())
                        .property("timebucket", 1252800000L)
                        .build(),
                new Entity.Builder()
                        .group("Entity")
                        .vertex("1")
                        .property("count", 1)
                        .property("timestamps", new RBMBackedTimestampSet.Builder()
                                .timeBucket(CommonTimeUtil.TimeBucket.HOUR)
                                .timestamp(Instant.ofEpochMilli(1254192989000L))
                                .build())
                        .property("timebucket", 1252800000L)
                        .build(),
                new Entity.Builder()
                        .group("Entity")
                        .vertex("4")
                        .property("count", 1)
                        .property("timestamps", new RBMBackedTimestampSet.Builder()
                                .timeBucket(CommonTimeUtil.TimeBucket.HOUR)
                                .timestamp(Instant.ofEpochMilli(1254192990000L))
                                .build())
                        .property("timebucket", 1252800000L)
                        .build()
        ), elements);
    }

}
