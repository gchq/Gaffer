/*
 * Copyright 2016-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.hbasestore.coprocessor.processor;

import uk.gov.gchq.gaffer.hbasestore.serialisation.LazyElementCell;

import java.util.List;
import java.util.function.Predicate;

public abstract class FilterProcessor implements GafferScannerProcessor, Predicate<LazyElementCell> {
    @Override
    public List<LazyElementCell> process(final List<LazyElementCell> elementCells) {
        // If we filter out a deleted element when compacting then the deleted
        // flag will not be persisted and the element will not get deleted.
        // When querying, deleted cells will have already been filtered out.
        elementCells.removeIf(elementCell -> !elementCell.isDeleted() && !test(elementCell));

        return elementCells;
    }
}
