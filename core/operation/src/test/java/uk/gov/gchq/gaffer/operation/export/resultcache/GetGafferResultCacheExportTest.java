/*
 * Copyright 2016-2021 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.export.resultcache;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.impl.export.resultcache.GetGafferResultCacheExport;

import static org.assertj.core.api.Assertions.assertThat;

public class GetGafferResultCacheExportTest extends OperationTest<GetGafferResultCacheExport> {
    @Test
    public void shouldJSONSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final String key = "key";
        final GetGafferResultCacheExport op = new GetGafferResultCacheExport.Builder()
                .key(key)
                .build();

        // When
        final byte[] json = JSONSerialiser.serialise(op, true);
        final GetGafferResultCacheExport deserialisedOp = JSONSerialiser.deserialise(json, GetGafferResultCacheExport.class);

        // Then
        assertThat(deserialisedOp.getKey()).isEqualTo(key);
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // When
        final String key = "key";
        final GetGafferResultCacheExport op = new GetGafferResultCacheExport.Builder()
                .key(key)
                .build();

        // Then
        assertThat(op.getKey()).isEqualTo(key);
    }

    @Test
    @Override
    public void shouldShallowCloneOperationREVIEWMAYBEDELETE() {
        // Given
        final String key = "key";
        final String jobId = "jobId";
        final GetGafferResultCacheExport getGafferResultCacheExport = new GetGafferResultCacheExport.Builder()
                .key(key)
                .jobId(jobId)
                .build();

        // When
        final GetGafferResultCacheExport clone = getGafferResultCacheExport.shallowClone();

        // Then
        assertThat(clone).isNotSameAs(getGafferResultCacheExport);
        assertThat(clone.getKey()).isEqualTo(key);
        assertThat(clone.getJobId()).isEqualTo(jobId);
    }

    @Test
    public void shouldGetOutputClass() {
        // When
        final Class<?> outputClass = getTestObjectOld().getOutputClass();

        // Then
        assertThat(outputClass).isEqualTo(Iterable.class);
    }

    @Override
    protected GetGafferResultCacheExport getTestObjectOld() {
        return new GetGafferResultCacheExport();
    }
}
