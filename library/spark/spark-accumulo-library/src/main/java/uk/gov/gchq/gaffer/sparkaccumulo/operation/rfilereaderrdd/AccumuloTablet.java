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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.rfilereaderrdd;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.io.Text;
import org.apache.spark.Partition;

import java.util.HashSet;
import java.util.Set;

/**
 * An {@code AccumuloTablet} maintains information related to a particular tablet, including the start and end
 * of that tablet and the set of files that data for the tablet is stored. It also includes a partition number to
 * identify which partition this tablet is in.
 */
public class AccumuloTablet implements Partition {
    private final int rddId;
    private final int index;
    private final String start;
    private final String end;
    private final Set<String> files;

    public AccumuloTablet(final int rddId,
                          final int index,
                          final String start,
                          final String end) {
        this.rddId = rddId;
        this.index = index;
        this.start = start;
        this.end = end;
        this.files = new HashSet<>();
    }

    public void addRFile(final String rFile) {
        files.add(rFile);
    }

    @Override
    public int index() {
        return index;
    }

    public Text getStartRow() {
        if (null == start) {
            return null;
        }
        return new Text(start);
    }

    public Text getEndRow() {
        if (null == end) {
            return null;
        }
        return new Text(end);
    }

    public Set<String> getFiles() {
        return files;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if (null == obj || getClass() != obj.getClass()) {
            return false;
        }

        final AccumuloTablet accumuloTablet = (AccumuloTablet) obj;

        return new EqualsBuilder()
                .append(rddId, accumuloTablet.rddId)
                .append(index, accumuloTablet.index)
                .append(start, accumuloTablet.start)
                .append(end, accumuloTablet.end)
                .append(files, accumuloTablet.files)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(13, 31)
                .append(rddId)
                .append(index)
                .append(start)
                .append(end)
                .append(files)
                .hashCode();
    }
}
