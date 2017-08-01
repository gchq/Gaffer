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
package uk.gov.gchq.gaffer.doc.properties.walkthrough;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.roaringbitmap.RoaringBitmap;
import uk.gov.gchq.gaffer.doc.walkthrough.AbstractWalkthrough;
import uk.gov.gchq.gaffer.doc.walkthrough.AbstractWalkthroughRunner;
import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.gaffer.types.TypeSubTypeValue;
import uk.gov.gchq.gaffer.types.TypeValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;

public class PropertiesWalkthroughRunner extends AbstractWalkthroughRunner {
    private static final List<Class<?>> SIMPLE_EXAMPLES = Arrays.asList(
            String.class,
            Long.class,
            Integer.class,
            Double.class,
            Float.class,
            Byte[].class,
            Boolean.class,
            Date.class,
            TypeValue.class,
            TypeSubTypeValue.class,
            FreqMap.class,
            HashMap.class,
            TreeSet.class,
            HyperLogLogPlus.class,
            RoaringBitmap.class
    );

    private static final List<AbstractWalkthrough> EXAMPLES = getExamples();

    private static List<AbstractWalkthrough> getExamples() {
        final List<AbstractWalkthrough> examples = new ArrayList<>();
        SIMPLE_EXAMPLES.forEach(c -> examples.add(new SimpleProperty(c)));
        examples.addAll(Arrays.asList(
                new DoublesUnion(),
                new LongsSketch(),
                new UnionSketch(),
                new ReservoirItemsUnion(),
                new TimestampSet(),
                new BoundedTimestampSet()
        ));

        return examples;
    }

    public static void main(final String[] args) throws Exception {
        new PropertiesWalkthroughRunner().run();
    }

    public PropertiesWalkthroughRunner() {
        super(EXAMPLES, "doc", "properties");
    }
}
