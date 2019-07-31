FROM python:3
COPY entrypoint.py /
COPY DataInputStream.py /
RUN pip install pandas
RUN pip install numpy
RUN pip install scipy
RUN pip install sh
ARG modulesName
COPY test/$modulesName.txt /
RUN pip install -r $modulesName.txt
ARG scriptName
ENV envVarScriptName $scriptName
COPY test/$scriptName.py /
ENTRYPOINT "python" "entrypoint.py" $envVarScriptName