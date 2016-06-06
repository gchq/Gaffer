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

package gaffer.accumulostore.export;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import gaffer.accumulostore.AccumuloProperties;
import gaffer.data.element.Element;
import gaffer.graph.export.GafferExporter;
import gaffer.user.User;
import java.util.HashSet;
import java.util.Set;

public class AccumuloStoreExporter extends GafferExporter {
    private Set<String> tableNames = new HashSet<>();

    public AccumuloStoreExporter() {
    }

    public String getTableNamePrefix() {
        return getStoreProperties().getTable() + KEY_SEPARATOR + getUserTimestampedExportName();
    }

    public Set<String> getTableNames() {
        return tableNames;
    }

    @Override
    protected void validateAdd(final String key, final Iterable<Element> elements, final User user) {
        // no validation required.
    }

    @Override
    protected void validateGet(final String key, final User user, final int start, final int end) {
        // no validation required.
    }

    @Override
    protected AccumuloProperties createExportStorePropsWithKey(final String key) {
        final AccumuloProperties props = getStoreProperties().clone();
        props.setTable(getTableName(key));
        return props;
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE", justification = "The properties should always be AccumuloProperties")
    @Override
    protected AccumuloProperties getStoreProperties() {
        return (AccumuloProperties) super.getStoreProperties();
    }

    private String getTableName(final String key) {
        final String tableName = getTableNamePrefix() + KEY_SEPARATOR + key;
        tableNames.add(tableName);
        return tableName;
    }
}
