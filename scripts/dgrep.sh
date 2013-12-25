#!/bin/bash


function usage
{
    echo "usage: dgrep.sh --key <<pattern>>"
    echo "usage: dgrep.sh --value <<pattern>>"
}

while [ $# > 0 ]; do
    case $1 in
	-k | --key )	       shift
				hasKey="true"
				key="$1"
				break
				;;
	-v | --value )		shift
				hasValue="true"
				value="$1"
				break
				;;
	-h | --help )		usage
				exit
				;;
	* )			usage
				exit 1
    esac
    shift
done


if [ "$hasKey" == "true" ]; then
	COMMAND="--key $key"
elif [ "$hasValue" == "true" ]; then
	COMMAND="--value $value"
fi

home_dir=`pwd`/..
echo "homedir is $home_dir.Command is $COMMAND"

java -cp  "$home_dir/lib/*" -server -Dlog4j.configuration=file:///$home_dir/config/grep.log4j.properties -Dapp.config=$home_dir/config/grep.config -XX:+PrintGCDateStamps edu.uiuc.helium.grep.GrepClient $COMMAND
