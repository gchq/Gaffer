/**
 * Copyright 2015 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gaffer.statistics.transform.impl;

import gaffer.statistics.SetOfStatistics;
import gaffer.statistics.Statistic;
import gaffer.statistics.transform.StatisticsTransform;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A {@link StatisticsTransform} that transforms a {@link SetOfStatistics} by either removing {@link Statistic}s
 * or only keeping the provided {@link Statistic}s.
 */
public class StatisticsRemoverByName extends StatisticsTransform {

    private static final long serialVersionUID = 8690209810619372280L;

    public enum KeepRemove {KEEP, REMOVE}
    private KeepRemove keepRemove;
    private Set<String> names = new HashSet<String>();

    public StatisticsRemoverByName() { }

    /**
     * Statistics with names in the provided {@link Set} will either be kept or
     * removed depending on the value of the enum {@link KeepRemove}.
     *
     * @param keepRemove
     * @param names
     */
    public StatisticsRemoverByName(KeepRemove keepRemove, Set<String> names) {
        this.keepRemove = keepRemove;
        this.names.addAll(names);
    }

    /**
     * Statistics with names in the provided {@link Set} will either be kept or
     * removed depending on the value of the enum {@link KeepRemove}.
     *
     * @param keepRemove
     * @param names
     */
    public StatisticsRemoverByName(KeepRemove keepRemove, String... names) {
        this.keepRemove = keepRemove;
        Collections.addAll(this.names, names);
    }

    @Override
    public SetOfStatistics transform(SetOfStatistics setOfStatistics) {
        if (keepRemove == KeepRemove.KEEP) {
            setOfStatistics.keepOnlyTheseStatistics(names);
        } else {
            setOfStatistics.removeTheseStatistics(names);
        }
        return setOfStatistics;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(keepRemove.ordinal());
        out.writeInt(names.size());
        for (String s : names) {
            Text.writeString(out, s);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        keepRemove = KeepRemove.values()[in.readInt()];
        names.clear();
        int num = in.readInt();
        for (int i = 0; i < num; i++) {
            names.add(Text.readString(in));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StatisticsRemoverByName that = (StatisticsRemoverByName) o;

        if (keepRemove != that.keepRemove) return false;
        if (names != null ? !names.equals(that.names) : that.names != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = keepRemove != null ? keepRemove.hashCode() : 0;
        result = 31 * result + (names != null ? names.hashCode() : 0);
        return result;
    }
}
