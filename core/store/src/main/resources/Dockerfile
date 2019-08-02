FROM python:3

COPY entrypoint.py /
COPY DataInputStream.py /

RUN pip install sh

COPY modules.txt /
RUN pip install -r modules.txt

ARG modulesName
COPY test/$modulesName.txt /
RUN pip install -r $modulesName.txt

ARG scriptName
ENV envVarScriptName $scriptName

ARG parameters
ENV parameters ${parameters}

COPY test/$scriptName.py /

ENTRYPOINT "python" "entrypoint.py" $envVarScriptName $parameters
