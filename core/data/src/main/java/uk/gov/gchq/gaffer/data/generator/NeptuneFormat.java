/*
 * Copyright 2022 Crown Copyright
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

package uk.gov.gchq.gaffer.data.generator;
public class NeptuneFormat extends CsvFormat {
    @Override
    public String getVertex() {
        return ":ID";
    }

    @Override
    public String getEntityGroup() {
        return ":LABEL";
    }

    @Override
    public String getEdgeGroup() {
        return ":TYPE";
    }

    @Override
    public String getSource() {
        return ":START_ID";
    }

    @Override
    public String getDestination() {
        return ":END_ID";
    }
}
