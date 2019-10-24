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
ENV scriptName $scriptName

ARG scriptParameters
ENV scriptParameters $scriptParameters

ARG scriptInputType
ENV scriptInputType $scriptInputType

COPY test/$scriptName.py /

ENTRYPOINT "python" "entrypoint.py" $scriptName $scriptParameters $scriptInputType
