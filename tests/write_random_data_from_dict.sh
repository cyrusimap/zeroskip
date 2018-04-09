#!/bin/bash
# set -x

PARAMS=""

DICTFILE="/usr/share/dict/words"
SHUF="/usr/bin/shuf"
ZSKIP="../tools/zeroskip"

if [[ $# < 2 ]]; then
    echo "Usage:"
    echo "  $0 - A tool for inserting random key value pairs into Zeroskip DB."
    echo "       The keys are picked randomly from /usr/share/dict/words"
    echo "  $0 -d [dbdir] -n [num-messages]"
    echo "  Other options:"
    echo "   -V | --verbose : Enable verbose mode"
    exit 1
fi

while (( "$#" )); do
  case "$1" in
    -n|--num-msgs)
      NUM_MSGS=$2
      shift 2
      ;;
    -d|--db)
      DBDIR=$2
      shift 2
      ;;
    -V|--verbose)
      VERBOSE=1
      shift 1
      ;;
    --) # end argument parsing
      shift
      break
      ;;
    -*|--*=) # Invalid argument
      echo "Error: Invalid argument $1" >&2
      exit 1
      ;;
    *) # preserve positional arguments
      PARAM="$PARAMS $1"
      shift
      ;;
  esac
done
# set positional arguments in their proper place
eval set -- "$PARAMS"

dbdir=$DBDIR
nummsgs=$NUM_MSGS

if [[ -z ${DBDIR+x} ]]; then
    echo "Need a valid database directory. (-d)"
    exit 1;
fi

if [[ -z ${NUM_MSGS+x} ]]; then
    echo "Need a value for the number of messages. (-n)"
    exit 1;
fi

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

pgbar()
{
	let _progress=(${1}*100/${2}*100)/100
	let _done=(${_progress}*4)/10
	let _left=40-$_done

	_done=$(printf "%${_done}s")
	_left=$(printf "%${_left}s")

        printf "\rAdding Records : [${_done// /▇}${_left// / }] ${_progress}%% Completed"
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

        if [[ $VERBOSE == 1 ]]; then
            ../tools/zeroskip set $dbdir "$KEY" "$VAL"
        else
            RET=$(../tools/zeroskip set $dbdir "$KEY" "$VAL")
            pgbar ${i} ${nummsgs}
        fi
    done
    echo ""
}

gen_key_val
