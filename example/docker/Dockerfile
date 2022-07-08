FROM gchq/gaffer-rest

COPY /data .
COPY ./startGaffer.sh .
COPY ./addElements.sh .

ENTRYPOINT ["./startGaffer.sh"]
