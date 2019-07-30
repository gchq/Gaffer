FROM python:3
ARG scriptName
ENV envVarScriptName $scriptName
ADD entrypoint.py /
ADD test/DataInputStream.py /
ADD test/$scriptName.py /
RUN pip install pandas
RUN pip install numpy
RUN pip install scipy
RUN pip install sh
ENTRYPOINT "python" "entrypoint.py" $envVarScriptName