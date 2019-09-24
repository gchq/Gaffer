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

package uk.gov.gchq.gaffer.python.operation;

import com.google.gson.Gson;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class BuildImageFromDockerfile {
    private static final Logger LOGGER = LoggerFactory.getLogger(BuildImageFromDockerfile.class);

    /**
     * Builds docker image from Dockerfile
     * @param scriptName the name of the python script being run
     * @param scriptParameters the parameters of the script being run
     * @param scriptInputType the type of input for the script
     * @param docker the docker client the script is being run on
     * @param pathAbsolutePythonRepo the absolute path for the python repo
     * @return docker image from Dockerfile
     * @throws DockerException should the docker encounter an error, this will be thrown
     * @throws InterruptedException should this fail, this will be thrown
     * @throws IOException this will be thrown if non-compliant data is sent
     */
    public String buildImage(final String scriptName, final Map<String, Object> scriptParameters, final ScriptInputType scriptInputType, final DockerClient docker, final String pathAbsolutePythonRepo) throws DockerException, InterruptedException, IOException {
        // Build an image from the Dockerfile
        String params = " ";
        if (scriptParameters != null) {
            Map<String, String> map = new HashMap<>();
            LOGGER.info(scriptParameters.keySet().toArray()[0].toString());
            for (final String current: scriptParameters.keySet()) {
                if (scriptParameters.get(current) != null) {
                    map.put(current, scriptParameters.get(current).toString());
                }
            }
            params = new Gson().toJson(map).replaceAll("\"", "'");
        }
        LOGGER.info("params = " + params);
        final String buildargs =
                "{\"scriptName\":\"" + scriptName + "\",\"scriptParameters\":\"" + params + "\"," +
                        "\"modulesName\":\"" + scriptName + "Modules" + "\",\"scriptInputType\":\"" + scriptInputType.toString() + "\"}";
        LOGGER.info(buildargs);
        final DockerClient.BuildParam buildParam = DockerClient.BuildParam.create("buildargs", URLEncoder.encode(buildargs, "UTF-8"));

        LOGGER.info("Building the image from the Dockerfile...");
        final AtomicReference<String> imageIdFromMessage = new AtomicReference<String>();
        LOGGER.info(pathAbsolutePythonRepo + "/../");
        LOGGER.info(Paths.get(pathAbsolutePythonRepo + "/../").toString());
        return docker.build(Paths.get(pathAbsolutePythonRepo + "/../"), "pythonoperation:" + scriptName, "Dockerfile", message -> {
            final String imageId = message.buildImageId();
            if (imageId != null) {
                imageIdFromMessage.set(imageId);
            }
            LOGGER.info(String.valueOf(message));
        }, buildParam);
    }

    public void buildFiles(final String pathAbsolutePythonRepo) {

        try {
            PrintWriter fileWriter = new PrintWriter(pathAbsolutePythonRepo + "/../Dockerfile");
            fileWriter.print("FROM python:3\n" +
                    "\n" +
                    "COPY entrypoint.py /\n" +
                    "COPY DataInputStream.py /\n" +
                    "\n" +
                    "RUN pip install sh\n" +
                    "\n" +
                    "COPY modules.txt /\n" +
                    "RUN pip install -r modules.txt\n" +
                    "\n" +
                    "ARG modulesName\n" +
                    "COPY test/$modulesName.txt /\n" +
                    "RUN pip install -r $modulesName.txt\n" +
                    "\n" +
                    "ARG scriptName\n" +
                    "ENV scriptName $scriptName\n" +
                    "\n" +
                    "ARG scriptParameters\n" +
                    "ENV scriptParameters $scriptParameters\n" +
                    "\n" +
                    "ARG scriptInputType\n" +
                    "ENV scriptInputType $scriptInputType\n" +
                    "\n" +
                    "COPY test/$scriptName.py /\n" +
                    "\n" +
                    "ENTRYPOINT \"python\" \"entrypoint.py\" $scriptName $scriptParameters $scriptInputType\n");
            fileWriter.close();
        } catch (final FileNotFoundException e) {
            e.printStackTrace();
        }

        try {
            PrintWriter fileWriter = new PrintWriter(pathAbsolutePythonRepo + "/../modules.txt");
            fileWriter.print("pandas\n" +
                    "numpy\n" +
                    "scipy");
            fileWriter.close();
        } catch (final FileNotFoundException e) {
            e.printStackTrace();
        }

        try {
            PrintWriter fileWriter = new PrintWriter(pathAbsolutePythonRepo + "/../entrypoint.py");
            fileWriter.print("import importlib\n" +
                    "import socket\n" +
                    "import struct\n" +
                    "import re\n" +
                    "import pandas\n" +
                    "import sys\n" +
                    "from ast import literal_eval\n" +
                    "\n" +
                    "from DataInputStream import DataInputStream\n" +
                    "\n" +
                    "# Dynamically import the script\n" +
                    "scriptNameParam = sys.argv[1]\n" +
                    "scriptName = importlib.import_module(scriptNameParam)\n" +
                    "print('scriptName is ', scriptName)\n" +
                    "scriptParameters = sys.argv[2]\n" +
                    "try:\n" +
                    "    dictParameters = literal_eval(scriptParameters)\n" +
                    "except SyntaxError:\n" +
                    "    dictParameters = dict()\n" +
                    "print('scriptParams is ', scriptParameters)\n" +
                    "scriptInputType = sys.argv[3]\n" +
                    "\n" +
                    "HOST = socket.gethostbyname(socket.gethostname())\n" +
                    "PORT = 80\n" +
                    "print('Listening for connections from host: ', socket.gethostbyname(\n" +
                    "    socket.gethostname()))  # 172.17.0.2\n" +
                    "\n" +
                    "with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:\n" +
                    "    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)\n" +
                    "    # Setup the port and get it ready for listening for connections\n" +
                    "    s.bind((HOST, PORT))\n" +
                    "    s.listen(1)\n" +
                    "    print('Waiting for incoming connections...')\n" +
                    "    conn, addr = s.accept()  # Wait for incoming connections\n" +
                    "    conn.sendall(struct.pack('?', True))\n" +
                    "    print('Connected to: ', addr)\n" +
                    "    dataReceived = False\n" +
                    "    while not dataReceived:\n" +
                    "        dis = DataInputStream(conn)\n" +
                    "        if dis:\n" +
                    "            dataReceived = True\n" +
                    "            rawData = None\n" +
                    "            currentPayload = dis.read_utf()\n" +
                    "            while currentPayload != bytes(']', encoding='utf-8'):\n" +
                    "                if rawData is None:\n" +
                    "                    rawData = bytes() + currentPayload\n" +
                    "                else:\n" +
                    "                    currentPayload = dis.read_utf()\n" +
                    "                    rawData += currentPayload\n" +
                    "\n" +
                    "            # Convert data into the right form based on scriptInputType\n" +
                    "            if scriptInputType == 'DATAFRAME':\n" +
                    "                data = pandas.read_json(rawData, orient=\"records\", dtype=\"Object\")\n" +
                    "            elif scriptInputType == 'JSON':\n" +
                    "                data = rawData.decode('utf-8')\n" +
                    "\n" +
                    "            # Run the script passing in the parameters\n" +
                    "            data = scriptName.run(data, dictParameters)\n" +
                    "\n" +
                    "            # Convert the data back into JSON\n" +
                    "            print('type of output data before is: ', type(data))\n" +
                    "            print('type of dataframe is: ', type(pandas.DataFrame([0])))\n" +
                    "            if isinstance(data, type(pandas.DataFrame([0]))):\n" +
                    "                data = pandas.DataFrame.to_json(data, orient=\"records\")\n" +
                    "\n" +
                    "            # Send the results back to the server\n" +
                    "            print('type of output data is: ', type(data))\n" +
                    "            i = 0\n" +
                    "            conn.sendall(struct.pack('>i', len(data)))\n" +
                    "            if len(data) > 65000:\n" +
                    "                splitData = re.findall(('.' * 65000), data)\n" +
                    "                while i < (len(data) / 65000) - 1:\n" +
                    "                    conn.sendall(struct.pack('>H', 65000))\n" +
                    "                    conn.sendall(splitData[i].encode('utf-8'))\n" +
                    "                    i += 1\n" +
                    "            conn.sendall(struct.pack('>H', len(data) % 65000))\n" +
                    "            conn.sendall(data[65000 * i:].encode('utf-8'))\n");
            fileWriter.close();
        } catch (final FileNotFoundException e) {
            e.printStackTrace();
        }

        try {
            PrintWriter fileWriter = new PrintWriter(pathAbsolutePythonRepo + "/." +
                    "./DataInputStream" +
                    ".py");
            fileWriter.print("\"\"\"\n" +
                    "Reading from Java DataInputStream format.\n" +
                    "\"\"\"\n" +
                    "\n" +
                    "import struct\n" +
                    "\n" +
                    "\n" +
                    "def recvall(sock, count):\n" +
                    "    buf = b''\n" +
                    "    while count:\n" +
                    "        newbuf = sock.recv(count)\n" +
                    "        if not newbuf:\n" +
                    "            return None\n" +
                    "        buf += newbuf\n" +
                    "        count -= len(newbuf)\n" +
                    "    return buf\n" +
                    "\n" +
                    "\n" +
                    "class DataInputStream:\n" +
                    "    def __init__(self, stream):\n" +
                    "        self.stream = stream\n" +
                    "\n" +
                    "    def read_boolean(self):\n" +
                    "        return struct.unpack('?', self.stream.recv(1))[0]\n" +
                    "\n" +
                    "    def read_byte(self):\n" +
                    "        return struct.unpack('b', self.stream.recv(1))[0]\n" +
                    "\n" +
                    "    def read_unsigned_byte(self):\n" +
                    "        return struct.unpack('B', self.stream.recv(1))[0]\n" +
                    "\n" +
                    "    def read_char(self):\n" +
                    "        return chr(struct.unpack('>H', self.stream.recv(2))[0])\n" +
                    "\n" +
                    "    def read_double(self):\n" +
                    "        return struct.unpack('>d', self.stream.recv(8))[0]\n" +
                    "\n" +
                    "    def read_float(self):\n" +
                    "        return struct.unpack('>f', self.stream.recv(4))[0]\n" +
                    "\n" +
                    "    def read_short(self):\n" +
                    "        return struct.unpack('>h', self.stream.recv(2))[0]\n" +
                    "\n" +
                    "    def read_unsigned_short(self):\n" +
                    "        return struct.unpack('>H', self.stream.recv(2))[0]\n" +
                    "\n" +
                    "    def read_long(self):\n" +
                    "        return struct.unpack('>q', self.stream.recv(8))[0]\n" +
                    "\n" +
                    "    def read_utf(self):\n" +
                    "        utf_length = struct.unpack('>H', self.stream.recv(2))[0]\n" +
                    "        return recvall(self.stream, utf_length)\n" +
                    "\n" +
                    "    def read_int(self):\n" +
                    "        return struct.unpack('>i', self.stream.recv(4))[0]\n");
            fileWriter.close();
        } catch (final FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
