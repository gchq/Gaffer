FROM python:3
COPY entrypoint.py /
COPY DataInputStream.py /
RUN pip install pandas
RUN pip install numpy
RUN pip install scipy
RUN pip install sh
ARG modulesName
RUN if [ $modulesName = ""]; then COPY test/$modulesName.txt; else echo $modulesName; fi;
ARG scriptName
ENV envVarScriptName $scriptName
COPY test/$scriptName.py /
ENTRYPOINT "python" "entrypoint.py" $envVarScriptName