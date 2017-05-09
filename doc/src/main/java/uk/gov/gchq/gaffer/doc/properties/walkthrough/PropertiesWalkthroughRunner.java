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

import uk.gov.gchq.gaffer.doc.walkthrough.AbstractWalkthrough;
import uk.gov.gchq.gaffer.doc.walkthrough.AbstractWalkthroughRunner;
import java.util.Arrays;
import java.util.List;

public class PropertiesWalkthroughRunner extends AbstractWalkthroughRunner {
    private static final List<Class<? extends AbstractWalkthrough>> EXAMPLES = Arrays.asList(
            DoublesUnion.class,
            LongsSketch.class,
            UnionSketch.class,
            ReservoirItemsUnion.class
    );

    public PropertiesWalkthroughRunner() {
        super(EXAMPLES, "doc", "properties");
    }

    public static void main(final String[] args) throws Exception {
        new PropertiesWalkthroughRunner().run();
    }
}
