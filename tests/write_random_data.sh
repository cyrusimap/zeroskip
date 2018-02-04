#!/bin/bash
# set -x

if [[ $# < 2 ]]; then
    echo "Usage:"
    echo "  $0 [dbdir] [num-messages]"
    exit 1
fi

dbdir=$1
nummsgs=$2

# Chec
# Generates a number between 0 and 9999
gen_length()
{
    local length=$(cat /dev/urandom | tr -dc '0-9' | fold -w 256 | head -n 1 | sed -e 's/^0*//' | head --bytes 3)
    if [ "$length" == "" ]; then
        length=1
    fi

    echo "$length"
}

gen_key_val()
{
    for i in `seq 1 $nummsgs`
    do
        local klen=$(gen_length)
        KEY=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w "$klen" | head -1)

        local vlen=$(gen_length)
        VAL=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w "$vlen" | head -1)

        RET=$(../tools/zeroskip set $dbdir "$KEY" "$VAL")
    done
}

gen_key_val
