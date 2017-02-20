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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.utils.java;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.utils.Pair;
import uk.gov.gchq.gaffer.data.element.Element;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ElementConverterFunction implements PairFlatMapFunction<Element, Key, Value>, Serializable {
    private static final long serialVersionUID = -3259752069724639102L;

    private final Broadcast<AccumuloElementConverter> converterBroadcast;

    public ElementConverterFunction(final Broadcast<AccumuloElementConverter> converterBroadcast) {
        this.converterBroadcast = converterBroadcast;
    }

    @Override
    public Iterator<Tuple2<Key, Value>> call(final Element e) throws Exception {
        final List<Tuple2<Key, Value>> tuples = new ArrayList<>(2);
        final Pair<Key> keys = converterBroadcast.value().getKeysFromElement(e);
        final Value value = converterBroadcast.value().getValueFromElement(e);
        tuples.add(new Tuple2<>(keys.getFirst(), value));
        final Key second = keys.getSecond();
        if (second != null) {
            tuples.add(new Tuple2<>(second, value));
        }
        return tuples.listIterator();
    }
}
