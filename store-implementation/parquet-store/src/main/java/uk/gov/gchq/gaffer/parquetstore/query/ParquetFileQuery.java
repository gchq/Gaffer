/*
 * Copyright 2018. Crown Copyright
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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.predicate.FilterPredicate;

import uk.gov.gchq.gaffer.commonutil.ToStringBuilder;

public class ParquetFileQuery {

    private Path file;
    private FilterPredicate filter;
    private boolean fullyApplied;

    public ParquetFileQuery(final Path file, final FilterPredicate filter, final boolean fullyApplied) {
        this.file = file;
        this.filter = filter;
        this.fullyApplied = fullyApplied;

    }

    public Path getFile() {
        return file;
    }

    public FilterPredicate getFilter() {
        return filter;
    }

    public boolean isFullyApplied() {
        return fullyApplied;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("file", file)
                .append("filter", filter)
                .append("fullyApplied", fullyApplied)
                .toString();
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final ParquetFileQuery other = (ParquetFileQuery) obj;

        return new EqualsBuilder()
                .append(file, other.file)
                .append(filter, other.filter)
                .append(fullyApplied, other.fullyApplied)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(file)
                .append(filter)
                .append(fullyApplied)
                .toHashCode();
    }
}
