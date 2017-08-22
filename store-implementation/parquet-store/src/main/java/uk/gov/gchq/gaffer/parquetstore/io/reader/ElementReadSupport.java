/*
 * Copyright 2017. Crown Copyright
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

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.parquetstore.utils.GafferGroupObjectConverter;
import java.util.Map;

/**
 * This class provides the {@link ReadSupport} required by the {@link ParquetElementReader} making use of the
 * {@link ElementRecordMaterialiser} to directly build the Gaffer Elements from the parquet types.
 */
public class ElementReadSupport extends ReadSupport<Element> {
    private final boolean isEntity;
    private final GafferGroupObjectConverter converter;

    public ElementReadSupport(final boolean isEntity, final GafferGroupObjectConverter converter) {
        super();
        this.isEntity = isEntity;
        this.converter = converter;
    }

    @Override
    public RecordMaterializer<Element> prepareForRead(final Configuration configuration, final Map<String, String> map,
                                                      final MessageType parquetSchema, final ReadContext readContext) {
        return new ElementRecordMaterialiser(parquetSchema, isEntity, converter);
    }

    @Override
    public ReadContext init(final InitContext context) {
        return new ReadContext(context.getFileSchema());
    }
}
