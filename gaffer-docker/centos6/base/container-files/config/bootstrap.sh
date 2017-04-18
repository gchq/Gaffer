#!/bin/bash

set -e
set -u

# Supervisord default params
SUPERVISOR_PARAMS='-c /etc/supervisord.conf'


# Create directories for supervisor's UNIX socket and logs (which might be missing
# as container might start with /data mounted from another data-container).
# mkdir -p /data/conf /data/run /data/logs
# chmod 777 /data/conf /data/run /data/logs

if [ "$(ls /config/init/)" ]; then
  for init in /config/init/*.sh; do
    . $init
  done
fi


# We have TTY, so probably an interactive container...
if test -t 0; then
  # Run supervisord detached...
  supervisord $SUPERVISOR_PARAMS
  
  # Some command(s) has been passed to container? Execute them and exit.
  # No commands provided? Run bash.
  if [[ $@ ]]; then 
    eval $@
  else 
    export PS1='[\u@\h : \w]\$ '
    /bin/bash
  fi

# Detached mode? Run supervisord in foreground, which will stay until container is stopped.
else
  # If some extra params were passed, execute them before.
  # @TODO It is a bit confusing that the passed command runs *before* supervisord, 
  #       while in interactive mode they run *after* supervisor.
  #       Not sure about that, but maybe when any command is passed to container,
  #       it should be executed *always* after supervisord? And when the command ends,
  #       container exits as well.
  if [[ $@ ]]; then 
    eval $@
  fi
  supervisord -n $SUPERVISOR_PARAMS
fi
