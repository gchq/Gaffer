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

package uk.gov.gchq.gaffer.parquetstore.operation.handler.utilities;

import org.apache.spark.Partitioner;
import scala.collection.Seq;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

public class SeqObjectPartitioner extends Partitioner {

    private int numPartitions;
    private Object[] splitPoints;

    public SeqObjectPartitioner(final int numPartitions,
                                final TreeSet<Seq<Object>> splitPoints) {
        this.numPartitions = numPartitions;
        this.splitPoints = new Object[splitPoints.size()];
        int i = 0;
        for (final Seq<Object> splitPoint : splitPoints) {
            this.splitPoints[i] = splitPoint;
            i++;
        }
    }

    @Override
    public int numPartitions() {
        return numPartitions;
    }

    @Override
    public int getPartition(final Object key) {
        final Seq<Object> keys = (Seq<Object>) key;
        int index = Arrays.binarySearch(splitPoints, keys, new Comparator<Object>() {
            @Override
            public int compare(final Object obj1, final Object obj2) {
                final Seq<Object> seq1 = (Seq<Object>) obj1;
                final Seq<Object> seq2 = (Seq<Object>) obj2;
                final Iterator<Object> seq1Iterator = scala.collection.JavaConversions.asJavaIterator(seq1.iterator());
                final Iterator<Object> seq2Iterator = scala.collection.JavaConversions.asJavaIterator(seq2.iterator());
                while (seq1Iterator.hasNext()) {
                    final Comparable o1 = (Comparable) seq1Iterator.next();
                    if (!seq2Iterator.hasNext()) {
                        throw new RuntimeException("Should be comparing two Seqs of equal size, got " + seq1 + " and " + seq2);
                    }
                    final Comparable o2 = (Comparable) seq2Iterator.next();
                    final int comparison = o1.compareTo(o2);
                    if (0 != comparison) {
                        return comparison;
                    }
                }
                return 0;
            }
        });
        index = index < 0 ? (index + 1) * -1 : index;
        return index;
    }
}
