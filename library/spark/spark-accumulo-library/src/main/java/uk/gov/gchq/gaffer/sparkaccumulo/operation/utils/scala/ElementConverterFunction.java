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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.utils.scala;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.TraversableOnce;
import scala.collection.mutable.ArrayBuffer;
import scala.runtime.AbstractFunction1;
import uk.gov.gchq.gaffer.accumulostore.key.AccumuloElementConverter;
import uk.gov.gchq.gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import uk.gov.gchq.gaffer.accumulostore.utils.Pair;
import uk.gov.gchq.gaffer.data.element.Element;
import java.io.Serializable;

public class ElementConverterFunction extends AbstractFunction1<Element, TraversableOnce<Tuple2<Key, Value>>> implements Serializable {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ElementConverterFunction.class);
    private static final long serialVersionUID = 2359835481339851648L;
    private Broadcast<AccumuloElementConverter> converterBroadcast;

    public ElementConverterFunction(final Broadcast<AccumuloElementConverter> converterBroadcast) {
        this.converterBroadcast = converterBroadcast;
    }

    @Override
    public TraversableOnce<Tuple2<Key, Value>> apply(final Element element) {
        final ArrayBuffer<Tuple2<Key, Value>> buf = new ArrayBuffer<>();
        Pair<Key> keys = new Pair<>();
        Value value = null;
        try {
            keys = converterBroadcast.value().getKeysFromElement(element);
            value = converterBroadcast.value().getValueFromElement(element);
        } catch (final AccumuloElementConversionException e) {
            LOGGER.error(e.getMessage(), e);
        }
        final Key first = keys.getFirst();
        if (first != null) {
            buf.$plus$eq(new Tuple2<>(first, value));
        }
        final Key second = keys.getSecond();
        if (second != null) {
            buf.$plus$eq(new Tuple2<>(second, value));
        }
        return buf;
    }
}
