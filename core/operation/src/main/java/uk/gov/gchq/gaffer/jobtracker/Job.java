/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.jobtracker;

import com.fasterxml.jackson.annotation.JsonIgnore;

import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationChainDAO;

import java.nio.charset.Charset;

/**
 * POJO containing details of a scheduled Gaffer job,
 * a {@link Repeat} and an Operation chain as a String.
 * To be used within the ExecuteJob for a ScheduledJob.
 */
public class Job {
    private static final String CHARSET_NAME = CommonConstants.UTF_8;
    private Repeat repeat;
    private String opChain;

    public Job() {
    }

    public Job(final Repeat repeat) {
        this.repeat = repeat;
    }

    public Job(final Repeat repeat, final String opChain) {
        this.repeat = repeat;
        this.opChain = opChain;
    }

    public Job(final Repeat repeat, final OperationChain<?> opChain) {
        this.repeat = repeat;
        try {
            if (opChain instanceof OperationChainDAO) {
                this.opChain = new String(JSONSerialiser.serialise(opChain), Charset.forName(CHARSET_NAME));
            } else {
                final OperationChainDAO dao = new OperationChainDAO(opChain.getOperations());
                this.opChain = new String(JSONSerialiser.serialise(dao), Charset.forName(CHARSET_NAME));
            }
        } catch (final SerialisationException se) {
            throw new IllegalArgumentException(se.getMessage());
        }
    }

    public Repeat getRepeat() {
        return repeat;
    }

    public void setRepeat(final Repeat repeat) {
        this.repeat = repeat;
    }

    public String getOpChain() {
        return opChain;
    }

    @JsonIgnore
    public OperationChain<?> getOpChainAsOperationChain() {
        try {
            return JSONSerialiser.deserialise(opChain, OperationChainDAO.class);
        } catch (final SerialisationException e) {
            throw new IllegalArgumentException("Unable to deserialise Job OperationChain ", e);
        }
    }

    public void setOpChain(final String opChain) {
        this.opChain = opChain;
    }
}
