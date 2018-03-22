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

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.apache.commons.lang3.time.DateUtils;

import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.generator.OneToManyElementGenerator;
import uk.gov.gchq.gaffer.types.FreqMap;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public abstract class RoadTrafficElementGenerator<T> implements OneToManyElementGenerator<T> {

    protected List<Entity> createCardinalities(final List<Edge> edges) {
        final List<Entity> cardinalities = new ArrayList<>(edges.size() * 2);

        for (final Edge edge : edges) {
            cardinalities.add(createCardinality(edge.getSource(), edge.getDestination(), edge));
            cardinalities.add(createCardinality(edge.getDestination(), edge.getSource(), edge));
        }

        return cardinalities;
    }

    protected Entity createCardinality(final Object source,
                                       final Object destination,
                                       final Edge edge) {
        final HyperLogLogPlus hllp = new HyperLogLogPlus(5, 5);
        hllp.offer(destination);

        return new Entity.Builder()
                .vertex(source)
                .group("Cardinality")
                .property("edgeGroup", CollectionUtil.treeSet(edge.getGroup()))
                .property("hllp", hllp)
                .property("count", 1L)
                .build();
    }

    protected Date getDate(final String dCountString, final String hour) {
        Date dCount = null;
        try {
            final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm");
            simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
            dCount = simpleDateFormat.parse(dCountString);
        } catch (final ParseException e) {
            // incorrect date format
        }

        if (null == dCount) {
            try {
                final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
                dCount = simpleDateFormat.parse(dCountString);
            } catch (final ParseException e) {
                // another incorrect date format
            }
        }

        if (null == dCount) {
            throw new IllegalArgumentException(String.format("Unable to parse date string: %s", dCountString));
        }

        return DateUtils.addHours(dCount, Integer.parseInt(hour));
    }

    protected long getTotalCount(final FreqMap freqmap) {
        long sum = 0;
        for (final Long count : freqmap.values()) {
            sum += count;
        }

        return sum;
    }

}
