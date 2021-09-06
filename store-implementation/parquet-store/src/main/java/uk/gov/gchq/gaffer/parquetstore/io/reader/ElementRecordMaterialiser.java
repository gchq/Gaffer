/*
 * Copyright 2017-2021 Crown Copyright
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
package uk.gov.gchq.gaffer.parquetstore.io.reader;

import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.parquetstore.io.reader.converter.GafferElementConverter;
import uk.gov.gchq.gaffer.parquetstore.utils.GafferGroupObjectConverter;

/**
 * This class is used by the {@link ElementReadSupport} to materialise the Gaffer {@link Element}'s directly from the
 * Parquet primitive types.
 */
public class ElementRecordMaterialiser extends RecordMaterializer<Element> {

    private GafferElementConverter root;

    public ElementRecordMaterialiser(final MessageType parquetSchema, final boolean isEntity, final GafferGroupObjectConverter converter) {
        this.root = new GafferElementConverter(isEntity, parquetSchema, converter);
    }

    @Override
    public Element getCurrentRecord() {
        return this.root.getCurrentRecord();
    }

    @Override
    public GroupConverter getRootConverter() {
        return this.root;
    }
}
