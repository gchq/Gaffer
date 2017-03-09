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
import org.apache.commons.lang3.time.DateUtils;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToOneElementGenerator;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DataGenerator8 extends OneToOneElementGenerator<String> {

    @Override
    public Element getElement(final String line) {
        final String[] t = line.split(",");
        final Edge.Builder edgeBuilder = new Edge.Builder()
                .group("data")
                .source(t[0])
                .dest(t[1])
                .directed(true)
                .property("visibility", t[3])
                .property("count", 1L);

        final Date startDate;
        try {
            startDate = DateUtils.truncate(new SimpleDateFormat("dd/MM/yyyy HH:mm").parse(t[2]), Calendar.DAY_OF_MONTH);
            edgeBuilder
                    .property("startDate", startDate)
                    .property("endDate", DateUtils.addDays(startDate, 1));
        } catch (ParseException e) {
            throw new IllegalArgumentException("Unable to parse date", e);
        }

        return edgeBuilder.build();
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an element is not an Entity it must be an Edge")
    @Override
    public String getObject(final Element element) {
        if (element instanceof Entity) {
            throw new UnsupportedOperationException();
        } else {
            final Edge edge = ((Edge) element);
            return edge.getSource() + "," + edge.getDestination() + "," + edge.getProperty("startDate") + "," + edge.getProperty("visibility");
        }
    }
}
