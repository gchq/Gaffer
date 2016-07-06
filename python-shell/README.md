Copyright 2016 Crown Copyright

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Python shell
============

This python shell connects to a Gaffer REST API and requires python 3.x

To start using the python shell you will need an instance of the REST API running.
You can start our example rest server (see example-rest/README.md) using the command:

```
mvn clean install -Pstandalone
```

Once this is running you can run the python example using the command:

```
python3 python-shell/src/main/python/example.py
```

Alternatively if you have you own REST API running that is authenticated with
PKI certificates then you can follow the pki example. Before using the example you
will need to export your PKI certificate into a .pem file:

```
python3 python-shell/src/main/python/examplePki.py
```
