FROM gchq/gaffer-rest

COPY ./addElements.sh .
COPY ./startGaffer.sh .
COPY ./addElements.json .

ENTRYPOINT ["./startGaffer.sh"]
