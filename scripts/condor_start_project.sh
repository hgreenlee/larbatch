#! /bin/bash
#------------------------------------------------------------------
#
# Purpose: A batch worker script for starting a sam project.
#
# Usage:
#
# condor_start_project.sh [options]
#
# --sam_user <arg>    - Specify sam user (default $GRID_USER).
# --sam_group <arg>   - Specify sam group (required).
# --sam_station <arg> - Specify sam station (required).
# --sam_defname <arg> - Sam dataset definition name (required).
# --sam_project <arg> - Sam project name (required).
# --logdir <arg>      - Specify log directory (optional). 
# -g, --grid          - Be grid-friendly.
# --recur             - Recursive input dataset (force snapshot).
# --init <path>       - Absolute path of environment initialization script (optional).
#
# End options.
#
# Created: H. Greenlee, 29-Aug-2012
#
#------------------------------------------------------------------

# Parse arguments.

SAM_USER=$GRID_USER
SAM_GROUP=""
SAM_STATION=""
SAM_DEFNAME=""
SAM_PROJECT=""
LOGDIR=""
GRID=0
RECUR=0
INIT=""
IFDH_OPT=""

while [ $# -gt 0 ]; do
  case "$1" in

    # Help.
    -h|--help )
      awk '/^# Usage:/,/^# End options/{print $0}' $0 | cut -c3- | head -n -2
      exit
      ;;

    # Sam user.
    --sam_user )
      if [ $# -gt 1 ]; then
        SAM_USER=$2
        shift
      fi
      ;;

    # Sam group.
    --sam_group )
      if [ $# -gt 1 ]; then
        SAM_GROUP=$2
        shift
      fi
      ;;

    # Sam station.
    --sam_station )
      if [ $# -gt 1 ]; then
        SAM_STATION=$2
        shift
      fi
      ;;

    # Sam dataset definition name.
    --sam_defname )
      if [ $# -gt 1 ]; then
        SAM_DEFNAME=$2
        shift
      fi
      ;;

    # Sam project name.
    --sam_project )
      if [ $# -gt 1 ]; then
        SAM_PROJECT=$2
        shift
      fi
      ;;

    # Log directory.
    --logdir )
      if [ $# -gt 1 ]; then
        LOGDIR=$2
        shift
      fi
      ;;

    # Grid flag.
    -g|--grid )
      GRID=1
      ;;

    # Recursive flag.
    --recur )
      RECUR=1
      ;;

    # Specify environment initialization script path.
    --init )
      if [ $# -gt 1 ]; then
        INIT=$2
        shift
      fi
      ;;

    # Other.
    * )
      echo "Unknown option $1"
      exit 1
  esac
  shift
done

# Done with arguments.

echo "Nodename: `hostname`"

# Check and print configuraiton options.

echo "Sam user: $SAM_USER"
echo "Sam group: $SAM_GROUP"
echo "Sam station: $SAM_STATION"
echo "Sam dataset definition: $SAM_DEFNAME"
echo "Sam project name: $SAM_PROJECT"

# Complain if SAM_STATION is not defined.

if [ x$SAM_STATION = x ]; then
  echo "Sam station was not specified (use option --sam_station)."
  exit 1
fi

# Complain if SAM_GROUP is not defined.

if [ x$SAM_GROUP = x ]; then
  echo "Sam group was not specified (use option --sam_group)."
  exit 1
fi

# Complain if SAM_DEFNAME is not defined.

if [ x$SAM_DEFNAME = x ]; then
  echo "Sam dataset was not specified (use option --sam_defname)."
  exit 1
fi

# Complain if SAM_PROJECT is not defined.

if [ x$SAM_PROJECT = x ]; then
  echo "Sam project name was not specified (use option --sam_project)."
  exit 1
fi

# Initialize ups products and mrb.

echo "Initializing ups and mrb."

if [ x$INIT != x ]; then
  if [ ! -f $INIT ]; then
    echo "Environment initialization script $INIT not found."
    exit 1
  fi
  echo "Sourcing $INIT"
  source $INIT
else
  echo "Sourcing setup_experiment.sh"
  source ${CONDOR_DIR_INPUT}/setup_experiment.sh
fi

echo PRODUCTS=$PRODUCTS

# Ifdh may already be setup by jobsub wrapper.
# If not, set it up here.

echo "IFDHC_DIR=$IFDHC_DIR"
if [ x$IFDHC_DIR = x ]; then
  echo "Setting up ifdhc, because jobsub did not set it up."
  setup ifdhc
fi
echo "IFDHC_DIR=$IFDHC_DIR"

# Set options for ifdh.

#if [ $GRID -ne 0 ]; then
#  echo "X509_USER_PROXY = $X509_USER_PROXY"
#  if ! echo $X509_USER_PROXY | grep -q Production; then
#    IFDH_OPT="--force=expgridftp"
#  fi
#fi
echo "IFDH_OPT=$IFDH_OPT"

# Create the scratch directory in the condor scratch diretory.
# Copied from condor_lBdetMC.sh.
# Scratch directory path is stored in $TMP.
# Scratch directory is automatically deleted when shell exits.

# Do not change this section.
# It creates a temporary working directory that automatically cleans up all
# leftover files at the end.
TMP=`mktemp -d ${_CONDOR_SCRATCH_DIR}/working_dir.XXXXXXXXXX`
TMP=${TMP:-${_CONDOR_SCRATCH_DIR}/working_dir.$$}

{ [[ -n "$TMP" ]] && mkdir -p "$TMP"; } || \
  { echo "ERROR: unable to create temporary directory!" 1>&2; exit 1; }
trap "[[ -n \"$TMP\" ]] && { cd ; rm -rf \"$TMP\"; }" 0
cd $TMP
# End of the section you should not change.

echo "Scratch directory: $TMP"

# See if we need to set umask for group write.

if [ $GRID -eq 0 -a x$LOGDIR != x ]; then
  LOGUSER=`stat -c %U $LOGDIR`
  CURUSER=`whoami`
  if [ $LOGUSER != $CURUSER ]; then
    echo "Setting umask for group write."
    umask 002
  fi
fi

# Save the project name in a file.

echo $SAM_PROJECT > sam_project.txt

# If recursive flag, take snapshot of input dataset.

if [ $RECUR -ne 0 ]; then
  echo "Forcing snapshot"
  SAM_DEFNAME=${SAM_DEFNAME}:force
fi

# Start the project.

nostart=1
echo "Starting project ${SAM_PROJECT}."
ifdh startProject $SAM_PROJECT $SAM_STATION $SAM_DEFNAME $SAM_USER $SAM_GROUP
if [ $? -eq 0 ]; then
  echo "Project successfully started."
  nostart=0
else
  echo "Start project error status $?"
fi

# Check the project snapshot.

nf=0
if [ $nostart -eq 0 ]; then
  nf=`ifdh translateConstraints "snapshot_for_project_name $SAM_PROJECT" | wc -l`
  echo "Project snapshot contains $nf files."
fi

# Abort if snapshot contains zero files.  Stop project and eventually exit with error status.

if [ $nostart -eq 0 -a $nf -eq 0 ]; then
  echo "Stopping project."
  nostart=1
  PURL=`ifdh findProject $SAM_PROJECT $SAM_STATION`
  if [ x$PURL != x ]; then
    echo "Project url: $PURL"
    ifdh endProject $PURL
    if [ $? -eq 0 ]; then
      echo "Project successfully stopped."
    fi
  fi
fi

# Stash all of the files we want to save in a local
# directory with a unique name.  Then copy this directory
# and its contents recursively.

if [ x$LOGDIR != x ]; then
  LOGDIR=`echo $LOGDIR | sed 's/@s/sam/'`
  OUTPUT_SUBDIR=${CLUSTER}_start
  mkdir $OUTPUT_SUBDIR
  for outfile in *; do
    if [ $outfile != $OUTPUT_SUBDIR ]; then
      mv $outfile $OUTPUT_SUBDIR
    fi
  done
  echo "ifdh cp -r $IFDH_OPT $OUTPUT_SUBDIR ${LOGDIR}/$OUTPUT_SUBDIR"
  ifdh cp -r $IFDH_OPT $OUTPUT_SUBDIR ${LOGDIR}/$OUTPUT_SUBDIR
  stat=$?
  if [ $stat -ne 0 ]; then
    echo "ifdh cp failed with status ${stat}."
    exit $stat
  fi 
fi

# Done.  Set exit status to reflect whether project was started (0=success).

exit $nostart
