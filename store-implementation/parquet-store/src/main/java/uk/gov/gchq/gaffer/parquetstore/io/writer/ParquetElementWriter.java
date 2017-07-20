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
package uk.gov.gchq.gaffer.parquetstore.io.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.parquetstore.utils.GafferGroupObjectConverter;

import java.io.IOException;

public class ParquetElementWriter extends ParquetWriter<Element> {

    public static Builder builder(final Path file) {
        return new Builder(file);
    }

    /**
     * Create a new {@link ParquetElementWriter}.
     *
     * @param file The file name to write to.
     * @param writeSupport The schema to write with.
     * @param compressionCodecName Compression code to use, or CompressionCodecName.UNCOMPRESSED
     * @param blockSize the block size threshold.
     * @param pageSize See parquet write up. Blocks are subdivided into pages for alignment and other purposes.
     * @param enableDictionary Whether to use a dictionary to compress columns.
     * @param enableValidation Whether to validate the data written
     * @param writerVersion Which parquet writer version to use
     * @param conf The Configuration to use.
     * @throws IOException thrown if the file does not exist
     */
    ParquetElementWriter(final Path file, final WriteSupport<Element> writeSupport,
                         final CompressionCodecName compressionCodecName,
                         final int blockSize, final int pageSize, final boolean enableDictionary,
                         final boolean enableValidation,
                         final ParquetProperties.WriterVersion writerVersion,
                         final Configuration conf)
            throws IOException {
        super(file, writeSupport, compressionCodecName, blockSize, pageSize,
                pageSize, enableDictionary, enableValidation, writerVersion, conf);
    }

    public static class Builder extends ParquetWriter.Builder<Element, Builder> {
        private MessageType type = null;
        private boolean isEntity = true;
        private GafferGroupObjectConverter converter = null;

        public Builder(final Path file) {
            super(file);
        }

        public Builder withType(final MessageType type) {
            this.type = type;
            return this;
        }

        public Builder isEntity(final boolean isEntity) {
            this.isEntity = isEntity;
            return this;
        }

        public Builder usingConverter(final GafferGroupObjectConverter converter) {
            this.converter = converter;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        protected WriteSupport<Element> getWriteSupport(final Configuration conf) {
            return new ElementWriteSupport(type, isEntity, converter);
        }
    }
}
