FROM python:3
ARG scriptName
ADD entrypoint.py /
ADD test/$scriptName.py /
RUN pip install pandas
RUN pip install numpy
RUN pip install scipy
RUN pip install sh
ENTRYPOINT [ "python", "entrypoint.py", "script2"]