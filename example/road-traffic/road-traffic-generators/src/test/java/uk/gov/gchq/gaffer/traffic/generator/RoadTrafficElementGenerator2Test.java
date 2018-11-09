/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.traffic.generator;

import com.google.common.collect.Lists;
import org.apache.commons.io.LineIterator;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.serialisation.json.SimpleClassNameCache;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

public class RoadTrafficElementGenerator2Test {

    @Test
    public void shouldParseSampleData() throws IOException {
        // Given
        final RoadTrafficCsvElementGenerator2 generator2 = new RoadTrafficCsvElementGenerator2();

        // When
        final List<Element> elements2;
        try (final InputStream inputStream = createInputStream()) {
            elements2 = Lists.newArrayList(generator2.apply(() -> new LineIterator(new InputStreamReader(inputStream))));
        }

        // Then - the results should be the same as those generated using the original element generator
        final RoadTrafficStringElementGenerator generator1 = new RoadTrafficStringElementGenerator();
        final List<Element> elements1;
        try (final InputStream inputStream = createInputStream()) {
            elements1 = Lists.newArrayList(generator1.apply(() -> new LineIterator(new InputStreamReader(inputStream))));
        }
        //TODO remove commented code
        JSONSerialiser.getMapper();
        SimpleClassNameCache.setUseFullNameForSerialisation(false);
        System.out.println(new String(JSONSerialiser.serialise(RoadTrafficCsvElementGenerator2.CSV_ELEMENT_GENERATOR, true)));
        elements1.forEach(e -> e.removeProperty("hllp"));
        elements2.forEach(e -> e.removeProperty("hllp"));
        ElementUtil.assertElementEquals(elements1, elements2);
    }

    private InputStream createInputStream() {
        return StreamUtil.openStream(getClass(), "/roadTrafficSampleData.csv");
    }

}
