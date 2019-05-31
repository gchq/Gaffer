/*
 * Copyright 2018-2019 Crown Copyright
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

import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.io.api.Binary;
import org.junit.Test;

import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class ParquetFileQueryTest {

    @Test
    public void testEqualsAndHashCode() {
        // Given
        final ParquetFileQuery q1 = new ParquetFileQuery(new Path("1"),
                eq(FilterApi.binaryColumn("A"), Binary.fromString("T")), true);
        final ParquetFileQuery q2 = new ParquetFileQuery(new Path("1"),
                eq(FilterApi.binaryColumn("A"), Binary.fromString("T")), true);
        final ParquetFileQuery q3 = new ParquetFileQuery(new Path("1"),
                eq(FilterApi.binaryColumn("A"), Binary.fromString("T")), false);
        final ParquetFileQuery q4 = new ParquetFileQuery(new Path("1"),
                eq(FilterApi.binaryColumn("A"), Binary.fromString("X")), true);
        final ParquetFileQuery q5 = new ParquetFileQuery(new Path("1"),
                eq(FilterApi.binaryColumn("B"), Binary.fromString("T")), true);
        final ParquetFileQuery q6 = new ParquetFileQuery(new Path("2"),
                eq(FilterApi.binaryColumn("A"), Binary.fromString("T")), true);

        // When / Then
        assertEquals(q1, q2);
        assertEquals(q1.hashCode(), q2.hashCode());
        assertNotEquals(q1, q3);
        assertNotEquals(q1.hashCode(), q3.hashCode());
        assertNotEquals(q1, q4);
        assertNotEquals(q1.hashCode(), q4.hashCode());
        assertNotEquals(q1, q5);
        assertNotEquals(q1.hashCode(), q5.hashCode());
        assertNotEquals(q1, q6);
        assertNotEquals(q1.hashCode(), q6.hashCode());
    }
}
