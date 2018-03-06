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

package uk.gov.gchq.gaffer.spark.function;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.function.Function;

/**
 * A {@link Function} to convert a {@link Dataset} into an {@link Iterable} of
 * {@link Row}s.
 */
public class DataFrameToIterableRow implements Function<Dataset<Row>, Iterable<? extends Row>> {

    @Override
    public Iterable<? extends Row> apply(final Dataset<Row> dataset) {
        return dataset::toLocalIterator;
    }
}
