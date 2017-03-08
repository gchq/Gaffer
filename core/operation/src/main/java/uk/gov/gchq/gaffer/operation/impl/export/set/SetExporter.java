/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl.export.set;

import com.google.common.collect.Iterables;
import org.apache.commons.lang.builder.ToStringBuilder;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.LimitedCloseableIterable;
import uk.gov.gchq.gaffer.operation.impl.export.Exporter;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * A <code>SetExporter</code> is an in memory temporary {@link Exporter}
 * using a {@link Set}.
 * The values are stored in a {@link LinkedHashSet} in order to ensure there is
 * a predictable iteration order.
 */
public class SetExporter implements Exporter {
    private Map<String, Set<Object>> exports = new HashMap<>();

    @Override
    public void add(final String key, final Iterable<?> results) {
        Iterables.addAll(getExport(key), results);
    }

    @Override
    public CloseableIterable<?> get(final String key) {
        return get(key, 0, null);
    }

    public CloseableIterable<?> get(final String key, final int start, final Integer end) {
        return new LimitedCloseableIterable<>(getExport(key), start, end);
    }

    private Set<Object> getExport(final String key) {
        Set<Object> export = exports.get(key);
        if (null == export) {
            export = new LinkedHashSet<>();
            exports.put(key, export);
        }

        return export;
    }

    public String toString() {
        return new ToStringBuilder(this)
                .append("exports", exports)
                .toString();
    }


}
