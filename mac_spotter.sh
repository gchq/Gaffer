LAST_COUNT1=1
LAST_COUNT2=1
SLEEP_COUNT=1
SLEEP_COUNT_THESHOLD=5
while [ 1 -eq 1 ]
do
    if [ $SLEEP_COUNT -eq 1  ]
    then
        COUNT1=`find $HOMEAshG/Gaffer  -print 2>/dev/null | grep miniAccumuloStoreTest | wc -l`
        if [ "$COUNT1" -ne $LAST_COUNT1 ]
        then
            if [ $LAST_COUNT1 -eq 0 ]
            then
                echo ""
            fi
            echo "Count 1 at `date` is $COUNT1"
            LAST_COUNT1=$COUNT1
        fi
    fi
    COUNT2=`find /var/folders/7w -print 2>/dev/null | grep miniAccumuloStoreTest | wc -l`
    if [ $COUNT2 -ne $LAST_COUNT2 ]
    then
        if [ $LAST_COUNT2 -eq 0 ]
        then
            echo ""
        fi
        echo "Count 2 at `date` is $COUNT2"
        LAST_COUNT2=$COUNT2
    fi
    SLEEP_COUNT=`expr $SLEEP_COUNT + 1`
    if [ $SLEEP_COUNT -gt 5 ]
    then
        SLEEP_COUNT=1
    fi
    sleep 1
done
