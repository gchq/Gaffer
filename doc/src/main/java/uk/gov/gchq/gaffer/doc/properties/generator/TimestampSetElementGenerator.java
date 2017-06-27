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
package uk.gov.gchq.gaffer.doc.properties.generator;

import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.generator.OneToManyElementGenerator;
import uk.gov.gchq.gaffer.time.RBMBackedTimestampSet;
import uk.gov.gchq.gaffer.time.TimestampSet;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class TimestampSetElementGenerator implements OneToManyElementGenerator<String> {
    // Fix the seed so that the results are consistent
    private static final Random RANDOM = new Random(123456789L);
    private static final Instant START_OF_2017 = ZonedDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneId.of("UTC")).toInstant();
    private static final int SECONDS_IN_YEAR = 365 * 24 * 60 * 60;

    @Override
    public Iterable<Element> _apply(final String line) {
        final Set<Element> elements = new HashSet<>();
        for (int i = 0; i < 25; i++) {
            final TimestampSet timestampSet = new RBMBackedTimestampSet(CommonTimeUtil.TimeBucket.MINUTE);
            timestampSet.add(START_OF_2017.plusSeconds(RANDOM.nextInt(SECONDS_IN_YEAR)));
            final Edge edge = new Edge.Builder()
                    .group("red")
                    .source("A")
                    .dest("B")
                    .property("timestampSet", timestampSet)
                    .build();
            elements.add(edge);
        }
        return elements;
    }
}
