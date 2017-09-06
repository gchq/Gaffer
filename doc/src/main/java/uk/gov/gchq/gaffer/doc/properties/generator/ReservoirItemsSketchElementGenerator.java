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

import com.yahoo.sketches.sampling.ReservoirItemsSketch;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToManyElementGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ReservoirItemsSketchElementGenerator implements OneToManyElementGenerator<String> {
    private static final char[] CHARS = new char[]{'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J'};
    // Fix the seed so that the results are consistent
    private static final Random RANDOM = new Random(123456789L);

    @Override
    public Iterable<Element> _apply(final String line) {
        final List<Element> elements = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            final ReservoirItemsSketch<String> reservoirStringsSketch = ReservoirItemsSketch.newInstance(20);
            reservoirStringsSketch.update(getRandomString());
            final Edge edge = new Edge.Builder()
                    .group("red")
                    .source("A")
                    .dest("B")
                    .property("stringsSample", reservoirStringsSketch)
                    .build();
            elements.add(edge);
        }
        for (int i = 0; i < 500; i++) {
            final Edge edge = new Edge.Builder()
                    .group("blue")
                    .source("X")
                    .dest("Y" + i)
                    .build();
            elements.add(edge);
            final ReservoirItemsSketch<String> reservoirStringsSketchX = ReservoirItemsSketch.newInstance(20);
            reservoirStringsSketchX.update("Y" + i);
            final Entity entityX = new Entity.Builder()
                    .group("blueEntity")
                    .vertex("X")
                    .property("neighboursSample", reservoirStringsSketchX)
                    .build();
            elements.add(entityX);
            final ReservoirItemsSketch<String> reservoirStringsSketchY = ReservoirItemsSketch.newInstance(20);
            reservoirStringsSketchY.update("X");
            final Entity entityY = new Entity.Builder()
                    .group("blueEntity")
                    .vertex("Y" + i)
                    .property("neighboursSample", reservoirStringsSketchY)
                    .build();
            elements.add(entityY);
        }
        return elements;
    }

    private static String getRandomString() {
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 10; i++) {
            builder.append(CHARS[RANDOM.nextInt(10)]);
        }
        return builder.toString();
    }
}
