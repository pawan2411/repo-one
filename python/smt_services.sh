#!/bin/bash

#start and stop the smt gunicorn WSGI service

#default values
logconfigd=conf/loggingd.conf
logconfig=conf/logging.conf
workers=10
host=0.0.0.0
port=5163
user=fundamentals #default user
timeout=300
gunicorn=deps/root/usr/bin/gunicorn
smtapp=service:app
daemonmode=0

usage() {
   if [[ -n "$1" ]] ; then
      error "$1\n"
   fi
   errorDie "Usage: $this -a <action> [-w <workers>]  [-t <timeout>] [-b <hostname>] [-p <port>] [-l <logconfig>] [-h] [-d]
   REQUIRED:
      action =
                * start          - Start gunicorn-WSGI smt service 
                  stop           - Stop gunicorn-WSGI smt service; works only for daemon process,
                                   but use 'kill -s TERM PID(master process only)' otherwise.
                  status         - Status of gunicorn-WSGI smt service \n"
}

ifErrorThenDie() {
   if [[ $? -ne 0 ]] ; then
      if [[ -n "$*" ]] ; then
         errorDie "$*"
      else
         errorDie "Exiting.\n"
      fi
   fi
}

error() {
   printf "$*" >&2
}

errorDie() {
   error "$*"
   exit 1
}

countNumProcesses() {
   tempprogram=$1
   if [[ -z "$tempprogram" ]] ; then
      errorDie "ERROR: countNumProcesses(): Argument 1 is blank\n"
   fi
   printf $(ps -fU $(getCurrentUser) | grep -v grep | grep -cP "$tempprogram")
}

getHostType() {
   lcHost=$(echo "$HOSTNAME" | tr "[:upper:]" "[:lower:]")
   if [[ $user != $(/usr/bin/whoami) ]] ; then
       printf "dev"
   elif [[ $lcHost =~ stage ]] ; then
      printf "staging"
   elif [[  $lcHost == "firea08" ||
            $lcHost == "fireb08" ||
            $lcHost == "firea09" ||
            $lcHost == "fireb09" ||
            $lcHost == "firea10" ||
            $lcHost == "fireb10" ]] ; then
     printf "production"
      return 0
   else
      return 1
   fi
}

validateEnvironment() {
   hostType=$(getHostType)
   printf "Environment: $hostType\n"
   ifErrorThenDie "ERROR: Unrecognized host $HOSTNAME\n"
   if [[ $hostType =~ "staging|production" ]] ; then
       if [[ $user != $(/usr/bin/whoami) ]] ; then
	   errorDie "ERROR: Only $user can run this program\n" \
               "       Become fundamentals user as \"sudo -u fundamentals -i\"\n"
       fi
   fi
}


getPPID() {
   tempprogram=$1
   user=$(getCurrentUser)
   printf "$(ps -o ppid,pid,command -U $user | grep -P "^\s*1\s.*$tempprogram" | awk '{print $2}')"
}

killTermCmd() {
   tempprogram=$1
   pid=$(getPPID "$tempprogram")
   if [[ -n "$pid" ]] ; then
      kill -s TERM $pid
   fi
}

kill9Cmd() {
   tempprogram=$1
   pid=$(getPPID "$tempprogram")
   if [[ -n "$pid" ]] ; then
      kill -9 $pid
   fi
}

verifyKill() {
   tempprogram=$1
   sleep 3
   numProcesses=$(countNumProcesses "$tempprogram")
   if [[ $numProcesses -ne 0 ]] ; then
      errorDie "ERROR: Attempt to kill $tempprogram failed, $numProcesses processes still alive.  Try again\n"
   else
      printf "$tempprogram stopped successfully\n"
   fi
}

expectFile() {
   if [[ -z "$1" ]] ; then
      errorDie "ERROR: expectFile(): no file given\n"
   elif [[ ! -f "$1" ]] ; then
      errorDie "ERROR: Expected file not found $1\n"
   fi
}


expectVariableIsSet() {
   argument=$1
   variable=$2
   if [[ -z "$variable" ]] ; then
      usage "ERROR: Required argument -$argument not specified\n"
   fi
}

getCurrentUser(){
   printf "$(/usr/bin/whoami)"
}

startSMTServices() {
   smtservice=$1
   numProcesses=$(countNumProcesses "$smtservice")
   if [[ $numProcesses -eq 0 ]] ; then
      if [[ $daemonmode -eq 1 ]] ; then
        cmd="./env.sh $smtservice $GUNICORNOPTIONS -D"
      else
        cmd="./env.sh $smtservice $GUNICORNOPTIONS"
      fi
      $cmd
      ifErrorThenDie "\nERROR: Unable to run '$cmd'"
   else
      error "WARNING: $smtservice ALREADY RUNNING\n"
   fi
}


stopSMTServices() {
    killTermCmd $(getPPID "$gunicorn")
}

printStatus() {
   tempprogram=$1
   user=$(getCurrentUser)
   startLogLine=$(ps -o ppid,pid,lstart,command -U $user | grep -P "^\s*1\s.*$tempprogram")
   if [[ -n "$startLogLine" ]] ; then
      numProcesses=$(countNumProcesses "$tempprogram")
      printf " PPID   PID                  STARTED COMMAND             : NUMBER OF PARENT + CHILD PROCESSES RUNNING\n"
      printf "$startLogLine: $numProcesses Processes\n"
   else
      printf "$tempprogram\tNOT RUNNING\n"
   fi
}

while getopts ":a:w:b:p:t:l:dh" optname
do
   case "$optname" in
      "a") action="$OPTARG";;
      "w") workers="$OPTARG";;
      "b") host="$OPTARG";;
      "p") port="$OPTARG";;
      "d") daemonmode=1;;
      "t") timeout="$OPTARG";;
      "l") logconfig="$OPTARG";;
      "h") usage;;
      "?")
         usage "ERROR: Unknown option $OPTARG";;
      ":")
         usage "ERROR: No value for option $OPTARG";;
      *)
         # Should not occur
         usage "ERROR: Unknown error while processing options";;
   esac
done

expectVariableIsSet "a" "$action"
environment=$(getHostType)

# set logging options 
if [[ $daemonmode -eq 1 ]] ; then
   LOGGINGOPTIONS=$logconfigd #daemonmode logging file onfig
else
   LOGGINGOPTIONS=$logconfig
fi

# set gunicorn options
if [[ "$environment" == "dev" ]] ; then
   GUNICORNOPTIONS="-w $workers -t $timeout -b $host:$port $smtapp --log-config $LOGGINGOPTIONS"
elif [[ "$environment" == "staging" ]] ; then
   GUNICORNOPTIONS="-w $workers -t $timeout -b $host:$port $smtapp --log-config $LOGGINGOPTIONS"
elif [[ "$environment" == "production" ]] ; then
   GUNICORNOPTIONS="-w $workers -t $timeout -b $host:$port $smtapp --log-config $LOGGINGOPTIONS"
fi

case $action in
start)
   validateEnvironment
   startSMTServices "$gunicorn"
   ;;
stop)
   validateEnvironment
   stopSMTServices
   kill9Cmd "$gunicorn" # force kill only if sig quit didn't work in stopSMTServices
   verifyKill "$gunicorn"
   ;;
status)
   printStatus "$gunicorn"
   ;;
*)
   # Should not occur
   usage "ERROR: Unknown option '$action' for -a";;
esac
