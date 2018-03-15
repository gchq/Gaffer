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

package uk.gov.gchq.gaffer.sparkaccumulo.integration.delete;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import uk.gov.gchq.gaffer.accumulostore.integration.delete.AbstractDeletedElementsIT;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.spark.data.generator.RowToElementGenerator;
import uk.gov.gchq.gaffer.spark.function.DataFrameToIterableRow;
import uk.gov.gchq.gaffer.spark.operation.dataframe.GetDataFrameOfElements;

public class GetDataFrameOfElementsDeletedElementsIT extends AbstractDeletedElementsIT<GetDataFrameOfElements, Dataset<Row>> {
    @Override
    protected GetDataFrameOfElements createGetOperation() {
        return new GetDataFrameOfElements.Builder().build();
    }

    @Override
    protected void assertElements(final Iterable<ElementId> expected, final Dataset<Row> actual) {
        ElementUtil.assertElementEquals(expected, new RowToElementGenerator().apply(new DataFrameToIterableRow().apply(actual)));
    }
}
