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
package uk.gov.gchq.gaffer.jobtracker;

import uk.gov.gchq.koryphe.Summary;

/**
 * Denotes the status of a Gaffer job.
 */
@Summary("The status of a job")
public enum JobStatus {

    /**
     * The Gaffer job has been submitted and is running.
     */
    RUNNING,

    /**
     * The Gaffer job has completed successfully.
     */
    FINISHED,

    /**
     * An error occured while executing the Gaffer job.
     */
    FAILED
}
