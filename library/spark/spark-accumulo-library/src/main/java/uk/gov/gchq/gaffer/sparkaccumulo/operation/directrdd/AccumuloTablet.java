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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.directrdd;

import org.apache.hadoop.io.Text;
import org.apache.spark.Partition;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * An <code>AccumuloTablet</code> maintains information related to a particular tablet, including the start and end
 * of that tablet and the set of files that data for the tablet is stored. It also includes a partition number to
 * identify which partition this tablet is in.
 */
public class AccumuloTablet implements Serializable, Partition {
//    private final String name;
    private final int rddId;
    private final int index;
    private final String start;
    private final String end;
    private final Set<String> files;

    public AccumuloTablet(//final String name,
                          final int rddId,
                          final int index,
                          final String start,
                          final String end) {
//        this.name = name;
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
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final AccumuloTablet that = (AccumuloTablet) o;

        if (rddId != that.rddId) {
            return false;
        }
        return index == that.index;
    }

    @Override
    public int hashCode() {
        int result = rddId;
        result = 31 * result + index;
        return result;
    }
}
