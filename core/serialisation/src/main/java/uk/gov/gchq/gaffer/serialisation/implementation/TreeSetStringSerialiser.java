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
package uk.gov.gchq.gaffer.serialisation.implementation;

import com.google.common.base.Splitter;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialisation;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * A <code>TreeSetStringSerialiser</code> is a serialiser for {@link TreeSet}s with
 * {@link String} values.
 */
public class TreeSetStringSerialiser implements Serialisation<TreeSet> {
    private static final long serialVersionUID = -8241328807929077861L;
    private static final String COMMA = "\\,";
    private static final String OPEN = "{";
    private static final String CLOSE = "}";

    @Override
    public boolean canHandle(final Class clazz) {
        return TreeSet.class.isAssignableFrom(clazz);
    }

    @Override
    public byte[] serialise(final TreeSet treeSet) throws SerialisationException {
        final StringBuilder builder = new StringBuilder(OPEN);
        final Iterator values = treeSet.iterator();
        if (values.hasNext()) {
            builder.append(values.next());
        }
        while (values.hasNext()) {
            builder.append(COMMA).append(values.next());
        }
        builder.append(CLOSE);

        try {
            return builder.toString().getBytes(CommonConstants.UTF_8);
        } catch (UnsupportedEncodingException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
    }

    @Override
    public TreeSet<String> deserialise(final byte[] bytes) throws SerialisationException {
        final String str;
        try {
            str = new String(bytes, CommonConstants.UTF_8);
        } catch (UnsupportedEncodingException e) {
            throw new SerialisationException(e.getMessage(), e);
        }

        final TreeSet<String> treeSet = new TreeSet<>();
        final Iterable<String> items = Splitter.on(COMMA)
                .omitEmptyStrings()
                .split(str.substring(1, str.length() - 1));
        for (final String item : items) {
            treeSet.add(item);
        }

        return treeSet;
    }

    @Override
    public TreeSet deserialiseEmptyBytes() {
        return new TreeSet<>();
    }

    @Override
    public boolean preservesObjectOrdering() {
        return true;
    }
}
