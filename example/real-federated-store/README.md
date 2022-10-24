Copyright 2022 Crown Copyright

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Real FederatedStore Example
=============

## Deployment
This example does not start or configure a real accumulo cluster. Without you having access to an Accumulo cluster you will not be able to use a Gaffer Accumulo Store.

Assuming you have Java 8, Maven and Git installed, you can build and run the latest version of Gaffer FederatedStore locally by doing the following:
```bash
# Clone the Gaffer repository, to reduce the amount you need to download this will only clone the master branch with a depth of 1 so there won't be any history.
git clone --depth 1 https://github.com/gchq/Gaffer.git #--branch master
cd Gaffer/example/real-federated-store/

# Run this script from this directory.
# This will download several maven dependencies such as tomcat.
./startRealFederatedStore.sh
```

The rest api will be deployed to localhost:8080/rest.


## Example Operations
Examples can be found at `have-a-go-at-operations`.