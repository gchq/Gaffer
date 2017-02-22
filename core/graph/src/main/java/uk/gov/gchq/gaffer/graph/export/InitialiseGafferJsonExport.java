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

package uk.gov.gchq.gaffer.graph.export;

import uk.gov.gchq.gaffer.operation.impl.export.initialise.InitialiseExport;

public class InitialiseGafferJsonExport extends InitialiseExport {
    public static final long DEFAULT_TIME_TO_LIVE = 24 * 60 * 60 * 1000;

    /**
     * Time to live in milliseconds
     */
    private Long timeToLive = DEFAULT_TIME_TO_LIVE;

    private String visibility;


    public InitialiseGafferJsonExport() {
        super(new GafferJsonExporter());
    }

    public InitialiseGafferJsonExport(final String key) {
        super(new GafferJsonExporter(), key);
    }

    @Override
    public GafferJsonExporter getExporter() {
        return (GafferJsonExporter) super.getExporter();
    }

    public Long getTimeToLive() {
        return timeToLive;
    }

    public void setTimeToLive(final Long timeToLive) {
        if (null != timeToLive && timeToLive > DEFAULT_TIME_TO_LIVE) {
            throw new IllegalArgumentException("Time to live must be less than or equal to: " + DEFAULT_TIME_TO_LIVE);
        }
        this.timeToLive = timeToLive;
    }

    public String getVisibility() {
        return visibility;
    }

    public void setVisibility(final String visibility) {
        this.visibility = visibility;
    }

    public abstract static class BaseBuilder<CHILD_CLASS extends BaseBuilder<?>>
            extends InitialiseExport.BaseBuilder<InitialiseGafferJsonExport, CHILD_CLASS> {
        public BaseBuilder() {
            super(new InitialiseGafferJsonExport());
        }

        public CHILD_CLASS timeToLive(final Long timeToLive) {
            getOp().setTimeToLive(timeToLive);
            return self();
        }

        public CHILD_CLASS visibility(final String visibility) {
            getOp().setVisibility(visibility);
            return self();
        }
    }

    public static final class Builder extends BaseBuilder<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }
}
