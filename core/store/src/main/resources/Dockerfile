FROM python:3
ADD entrypoint.py /
ADD DataInputStream.py /
RUN pip install pandas
RUN pip install numpy
RUN pip install scipy
RUN pip install sh
ARG scriptName
ENV envVarScriptName $scriptName
ARG parameters
ENV envVarScriptName $parameters
ADD test/$scriptName.py /
ENTRYPOINT "python" "entrypoint.py" $envVarScriptName