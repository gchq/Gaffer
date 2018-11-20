/*
 * Copyright 2017-2018. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.query;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParquetQuery {

    private final Map<String, List<ParquetFileQuery>> groupToQueries;

    public ParquetQuery() {
        this.groupToQueries = new HashMap<>();
    }

    public void add(final String group, final ParquetFileQuery fileQuery) {
        if (!groupToQueries.containsKey(group)) {
            groupToQueries.put(group, new ArrayList<>());
        }
        groupToQueries.get(group).add(fileQuery);
    }

    public boolean isEmpty() {
        return groupToQueries.isEmpty();
    }

    public List<ParquetFileQuery> getAllParquetFileQueries() {
        final List<ParquetFileQuery> all = new ArrayList<>();
        groupToQueries
                .entrySet()
                .stream()
                .flatMap(e -> e.getValue().stream())
                .forEach(all::add);
        return all;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("groupToQueries", groupToQueries)
                .toString();
    }
}
