/*
 * Copyright 2016-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.serialisation.implementation;

import com.google.common.base.Splitter;

import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesViaStringDeserialiser;

import java.util.Iterator;
import java.util.TreeSet;

/**
 * A {@code TreeSetStringSerialiser} is a serialiser for {@link TreeSet}s with
 * {@link String} values.
 */
public class TreeSetStringSerialiser extends ToBytesViaStringDeserialiser<TreeSet<String>> {
    private static final long serialVersionUID = -8241328807929077861L;
    private static final String COMMA = "\\,";
    private static final String OPEN = "{";
    private static final String CLOSE = "}";

    public TreeSetStringSerialiser() {
        super(CommonConstants.UTF_8);
    }

    @Override
    public boolean canHandle(final Class clazz) {
        return TreeSet.class.isAssignableFrom(clazz);
    }

    @Override
    protected String serialiseToString(final TreeSet<String> object) throws SerialisationException {
        final StringBuilder builder = new StringBuilder(OPEN);
        final Iterator values = object.iterator();
        if (values.hasNext()) {
            builder.append(values.next());
        }
        while (values.hasNext()) {
            builder.append(COMMA).append(values.next());
        }
        builder.append(CLOSE);
        return builder.toString();
    }

    @Override
    public TreeSet<String> deserialiseString(final String value) throws SerialisationException {

        final TreeSet<String> treeSet = new TreeSet<>();
        final Iterable<String> items = Splitter.on(COMMA)
                .omitEmptyStrings()
                .split(value.substring(1, value.length() - 1));
        for (final String item : items) {
            treeSet.add(item);
        }

        return treeSet;
    }

    @Override
    public TreeSet<String> deserialiseEmpty() {
        return new TreeSet<>();
    }

    @Override
    public boolean preservesObjectOrdering() {
        return true;
    }

    @Override
    public boolean isConsistent() {
        return true;
    }
}
