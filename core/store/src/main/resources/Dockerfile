FROM python:3
ARG scriptName
ENV envArgScriptName=$scriptName
ADD entrypoint.py /
ADD ./test/script2.py /
RUN pip install pandas
RUN pip install numpy
RUN pip install scipy
RUN pip install sh
ENTRYPOINT [ "python", "./entrypoint.py", $envArgScriptName]