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

package uk.gov.gchq.gaffer.operation;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;

public interface SeededGet<I_ITEM, O> extends Get<CloseableIterable<I_ITEM>, O> {
    /**
     * @return the {@link CloseableIterable} of input seeds (SEED_TYPE) for the operation.
     */
    CloseableIterable<I_ITEM> getSeeds();

    /**
     * @param seeds the {@link CloseableIterable} of input seeds (SEED_TYPE) for the operation.
     */
    void setSeeds(final CloseableIterable<I_ITEM> seeds);
}
