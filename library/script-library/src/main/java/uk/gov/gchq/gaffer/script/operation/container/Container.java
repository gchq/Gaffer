/*
 * Copyright 2019 Crown Copyright
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

package uk.gov.gchq.gaffer.script.operation.container;

import java.io.DataInputStream;
import java.io.IOException;

public interface Container {

    /**
     * Send data to the container
     *
     * @param data             the data being sent
     * @throws Exception       if it fails to send the data
     */
    void sendData(Iterable data) throws IOException;

    /**
     * Retrieve data from the container
     *
     * @return the data from the container
     */
    DataInputStream receiveData();

    /**
     * Get the container id
     *
     * @return the container id
     */
    String getContainerId();

    int getPort();

}
