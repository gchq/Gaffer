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

package gaffer.operation;

import gaffer.commonutil.iterable.CloseableIterable;

/**
 * A <code>GetOperation</code> defines a seeded get operation to be processed on a graph.
 * <p>
 * Get operations have several flags to determine how to filter {@link gaffer.data.element.Element}s.
 *
 * @param <SEED_TYPE>   the seed type of the operation. This must be JSON serialisable.
 * @param <RETURN_TYPE> the result type of the operation. This must be JSON serialisable.
 */
public interface GetIterableElementsOperation<SEED_TYPE, RETURN_TYPE>
        extends GetIterableOperation<SEED_TYPE, RETURN_TYPE>,
        GetElementsOperation<SEED_TYPE, CloseableIterable<RETURN_TYPE>> {
    // Empty marker interface
}
