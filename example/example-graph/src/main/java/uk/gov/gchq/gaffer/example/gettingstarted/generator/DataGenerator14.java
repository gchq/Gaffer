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

package uk.gov.gchq.gaffer.example.gettingstarted.generator;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;

public class DataGenerator14 extends OneToOneElementGenerator<String> {

    @Override
    public Element getElement(final String line) {
        final String[] t = line.split(",");
        final Edge.Builder builder = new Edge.Builder()
                .group(t[2])
                .source(t[0])
                .dest(t[1])
                .directed(false)
                .property("count", 1);

        if ("yellow".equals(t[2])) {
            builder.property("score", t[3]);
        } else if ("green".equals(t[2])) {
            builder.directed(true);
        }

        return builder.build();
    }

    @Override
    public String getObject(final Element element) {
        throw new UnsupportedOperationException();
    }
}
