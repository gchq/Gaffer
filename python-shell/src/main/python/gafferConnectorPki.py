#
# Copyright 2016 Crown Copyright
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This module queries a Gaffer REST API with PKI authentication
"""

import ssl
import getpass
import urllib.request
import urllib.error

import gafferConnector


class GafferConnector(gafferConnector.GafferConnector):
    def __init__(self, host, pki, protocol=None, verbose=False):
        """
        This initialiser sets up a connection to the specified Gaffer server as per gafferConnector.GafferConnector and
        requires the additional pki object.
        """
        super().__init__(host=host, verbose=verbose)
        self._opener = urllib.request.build_opener(
            urllib.request.HTTPSHandler(self._host,
                                        context=pki.get_ssl_context(protocol)))


########################################################

class PkiCredentials:
    """
    This class holds a set of PKI credentials. These are loaded from a PEM file, which
    should contain the private key and the public keys for the entire certificate chain.
    """

    def __init__(self, cert_filename, password=None):
        """
        Construct the credentials class from a PEM file. If a password is not supplied
        and the file is password-protected then the password will be requested.
        """

        # Read the contents of the certificate file to check that it is readable
        with open(cert_filename, 'r') as cert_file:
            self._cert_file_contents = cert_file.read()
            cert_file.close()

        # Remember the filename
        self._cert_filename = cert_filename

        # Obtain the password if required and remember it
        if password is None:
            password = getpass.getpass('Password for PEM certificate file: ')
        self._password = password

    def get_ssl_context(self, protocol=None):
        """
        This method returns a SSL context based on the file that was specified
        when this object was created.

        Arguments:
         - An optional protocol. SSLv2 is used by default.

        Returns:
         - The SSL context
        """

        # Validate the arguments
        if protocol is None:
            protocol = ssl.PROTOCOL_SSLv2

        # Create an SSL context from the stored file and password.
        ssl_context = ssl.SSLContext(protocol)
        ssl_context.load_cert_chain(self._cert_filename,
                                    password=self._password)

        # Return the context
        return ssl_context

    def __str__(self):
        return 'Certificates from ' + self._cert_filename
