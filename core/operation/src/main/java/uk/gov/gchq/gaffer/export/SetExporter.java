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

package uk.gov.gchq.gaffer.export;

import com.google.common.collect.Iterables;
import org.apache.commons.lang.builder.ToStringBuilder;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.LimitedCloseableIterable;
import uk.gov.gchq.gaffer.user.User;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * A <code>SetExporter</code> is an in memory temporary {@link Exporter}
 * using a {@link Set}.
 * The values are stored in a {@link LinkedHashSet} in order to ensure there is
 * a predictable iteration order.
 */
public class SetExporter extends Exporter<Object> {
    private Set<Object> export = new LinkedHashSet<>();

    @Override
    public void initialise(final String key, final Object config, final User user) {
        super.initialise(key, config, user);
        export = new LinkedHashSet<>();
    }

    @Override
    protected void _add(final Iterable<?> values, final User user) {
        Iterables.addAll(export, values);
    }

    @Override
    protected CloseableIterable<?> _get(final User user, final int start, final int end) {
        return new LimitedCloseableIterable<>(export, start, end);
    }

    public Set<Object> getExport() {
        return export;
    }

    public void setExport(final Set<Object> export) {
        if (null == export) {
            this.export = new LinkedHashSet<>();
        } else {
            this.export = export;
        }
    }

    public String toString() {
        return new ToStringBuilder(this)
                .append(export)
                .toString();
    }
}
