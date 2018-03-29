#!/bin/bash
# set -x

DICTFILE="/usr/share/dict/words"
SHUF="/usr/bin/shuf"
ZSKIP="../tools/zeroskip"

if [[ $# < 2 ]]; then
    echo "Usage:"
    echo "  $0 - A tool for inserting random key value pairs into Zeroskip DB."
    echo "       The keys are picked randomly from /usr/share/dict/words"
    echo "  $0 [dbdir] [num-messages]"
    exit 1
fi

dbdir=$1
nummsgs=$2

UNAME=`uname`
if [[ $UNAME == "SunOS" ]]; then
          DICTFILE="/usr/share/lib/dict/words"
          SHUF="/opt/local/bin/shuf"
fi

if [ ! -f $DICTFILE ]; then
    # Check if /usr/share/dict/cracklib-small is available
    if [ ! -f "/usr/share/dict/cracklib-small" ]; then
        echo "$DICTFILE doesn't exist. This program only works if the file exists!"
        exit 1
    else
        DICTFILE="/usr/share/dict/cracklib-small"
        echo "Using $DICTFILE for words!"
    fi
fi

if [ ! -f $SHUF ]; then
    echo "$SHUF doesn't exist. Need $SHUF to run!"
    exit 1
fi

if [ ! -f $ZSKIP ]; then
    echo "$ZSKIP doesn't exist. Need $ZSKIP to run!"
    exit 1
fi

if [ ! -d "$dbdir" ]; then
    echo "DB $dbdir doesn't exist!"
    exit 1;
fi


# Generates a number between 0 and 9999
gen_length()
{
    local length=$(cat /dev/urandom | tr -dc '0-9' | fold -w 256 | head -n 1 | sed -e 's/^0*//' | head --bytes 3)
    if [ "$length" == "" ]; then
        length=1
    fi

    echo "$length"
}

gen_key_val_with_progress_bar()
{
    local tcount=$nummsgs

    already_done() { for ((done=0; done<elapsed; done=done+1)); do printf "▇"; done }
    remaining() { for ((remain=elapsed; remain<tcount; remain=remain+1)); do printf " "; done }
    percentage() { printf "| %s%%" $(( ((elapsed)*100)/(tcount)*100/100 )); }
    clean_line() { printf "\r"; }

    for (( elapsed=1; elapsed<=tcount; elapsed=elapsed+1 )); do
        already_done; remaining; percentage
        # Key from /usr/share/dict/words
        KEY=$($SHUF -n1 -r $DICTFILE)

        # Generate random value
        local vlen=$(gen_length)
        VAL=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w "$vlen" | head -1)

        RET=$(../tools/zeroskip set $dbdir "$KEY" "$VAL")

        clean_line
    done
    clean_line
    echo ""
}

gen_key_val()
{
    for i in `seq 1 $nummsgs`
    do
        # Key from /usr/share/dict/words
        KEY=$($SHUF -n1 -r $DICTFILE)

        # Generate random value
        local vlen=$(gen_length)
        VAL=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w "$vlen" | head -1)

        RET=$(../tools/zeroskip set $dbdir "$KEY" "$VAL")
    done
}

gen_key_val_with_progress_bar
