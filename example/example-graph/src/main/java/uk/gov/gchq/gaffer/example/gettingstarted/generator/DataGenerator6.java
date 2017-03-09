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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;

public class DataGenerator6 extends OneToOneElementGenerator<String> {

    @Override
    public Element getElement(final String line) {
        final String[] t = line.split(",");
        final Edge edge = new Edge("data");
        edge.setSource(t[0]);
        edge.setDestination(t[1]);
        edge.setDirected(true);

        final int count;
        if (t.length > 2 && null != t[2]) {
            count = Integer.parseInt(t[2]);
        } else {
            count = 1;
        }
        edge.putProperty("count", count);
        return edge;
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an element is not an Entity it must be an Edge")
    @Override
    public String getObject(final Element element) {
        if (element instanceof Entity) {
            throw new UnsupportedOperationException();
        } else {
            final Edge edge = ((Edge) element);
            return edge.getSource() + "," + edge.getDestination() + "," + edge.getProperty("count");
        }
    }
}
