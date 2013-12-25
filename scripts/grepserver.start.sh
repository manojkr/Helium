#!/bin/bash


function usage
{
    echo "usage: grepserver.start.sh"
}


home_dir=`pwd`/..
echo "home_dir is $home_dir"

java -cp "$home_dir/lib/*" -server -Dlog4j.configuration=file:///$home_dir/config/grep.log4j.properties -Dapp.config=$home_dir/config/grep.config edu.uiuc.helium.grep.GrepServer
