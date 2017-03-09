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

package uk.gov.gchq.gaffer.spark.operation;

import uk.gov.gchq.gaffer.operation.GetElementsOperation;

/**
 * Marker interface denoting operations which generate Spark RDDs from Accumulo.
 *
 * @param <SEED_TYPE>   the seed type of the operation. This must be JSON serialisable.
 * @param <RDD> the type of RDD to return
 */
public interface GetSparkRDDOperation<SEED_TYPE, RDD> extends GetElementsOperation<SEED_TYPE, RDD> {
    // Marker interface
}
