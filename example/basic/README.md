Copyright 2017 Crown Copyright

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Basic Demo
=============

## Deployment
Assuming you have Java 8, Maven and Git installed, you can build and run the latest version of the road traffic demo by doing the following:

```bash
# Clone the Gaffer repository, to reduce the amount you need to download this will only clone the master branch with a depth of 1 so there won't be any history.
git clone --depth 1 --branch master https://github.com/gchq/Gaffer.git
cd Gaffer

# Run the start script. This will download several maven dependencies such as tomcat.
# If you have a snapshot version and want to build all dependencies first then add -am as a script argument
./example/basic/scripts/start.sh
```

The rest api will be deployed to localhost:8080/rest.