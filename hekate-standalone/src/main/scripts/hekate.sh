#!/bin/sh
#***************************************************************************
# Java configuration options.
#***************************************************************************

#-------------------------------------------------
# JVM memory options (-Xms and -Xmx)
#-------------------------------------------------
# JAVA_MIN_MEM="128m"
# JAVA_MAX_MEM="512m"

#-------------------------------------------------
# Additional JVM options
#-------------------------------------------------
JAVA_OPTS="-server"

#-------------------------------------------------
# JMX remote access (non secure)
#-------------------------------------------------
# JMX_PORT="9010"

#-------------------------------------------------
# Application title in VisualVM (no whitespaces)
#-------------------------------------------------
# VISUAL_VM_TITLE=""


#***************************************************************************
# Startup script.
#***************************************************************************
#----- Resolve OS type. ----------------------
cygwin=false

case "`uname`" in
    CYGWIN*) cygwin=true
    ;;
esac

#----- Resolve HEKATE_HOME. ----------------------
# resolve links - $0 may be a softlink
PRG="$0"

while [ -h "$PRG" ]; do
    ls=`ls -ld "$PRG"`
    link=`expr "$ls" : '.*-> \(.*\)$'`
    if expr "$link" : '/.*' > /dev/null; then
        PRG="$link"
    else
        PRG=`dirname "$PRG"`/"$link"
    fi
done

# Get standard environment variables
PRGDIR=`dirname "$PRG"`

# Only set HEKATE_HOME if not already set
[ -z "$HEKATE_HOME" ] && HEKATE_HOME=`cd "$PRGDIR/.." >/dev/null; pwd`

# For Cygwin -> UNIX format
if ${cygwin}; then
    [ -n "$JAVA_HOME" ] && JAVA_HOME=`cygpath -u "$JAVA_HOME"`
    [ -n "$HEKATE_HOME" ] && HEKATE_HOME=`cygpath -u "$HEKATE_HOME"`
fi

#----- Resolve JAVA_HOME. ------------------------
if [ -z "$JAVA_HOME" ]; then
    JAVA_PATH=`which java 2>/dev/null`
    if [ "x$JAVA_PATH" != "x" ]; then
        JAVA_EXEC=${JAVA_PATH}
    fi
else
    JAVA_EXEC="${JAVA_HOME}/bin/java"
fi

if [ ! -f "$JAVA_EXEC" ]; then
    echo "JAVA_HOME environment variable is not defined."
    exit 1
fi

#----- Resolve CLASSPATH. ------------------------

CP_LIB="${HEKATE_HOME}/lib/*"

for file in ${HEKATE_HOME}/lib/*
do
    if [ -d ${file} ]; then
        CP_LIB="${CP_LIB}:${file}/*"
    fi
done

CLASSPATH=${CP_LIB}:${HEKATE_HOME}/config

#----- Resolve JVM options. ------------------
if [ -z "$JAVA_MIN_MEM" ] && [ ${JAVA_MAX_MEM} ]; then
    JAVA_MIN_MEM=${JAVA_MAX_MEM}
fi

if [ ${JAVA_MIN_MEM} ]; then
    ALL_JAVA_OPTS="$ALL_JAVA_OPTS -Xms${JAVA_MIN_MEM}"
fi

if [ ${JAVA_MAX_MEM} ]; then
    ALL_JAVA_OPTS="$ALL_JAVA_OPTS -Xmx${JAVA_MAX_MEM}"
fi


ALL_JAVA_OPTS="${ALL_JAVA_OPTS} ${JAVA_OPTS}"

if [ ${JMX_PORT} ]; then
    ALL_JAVA_OPTS="${ALL_JAVA_OPTS} -Dcom.sun.management.jmxremote.port=${JMX_PORT}"
    ALL_JAVA_OPTS="${ALL_JAVA_OPTS} -Dcom.sun.management.jmxremote"
    ALL_JAVA_OPTS="${ALL_JAVA_OPTS} -Dcom.sun.management.jmxremote.local.only=false"
    ALL_JAVA_OPTS="${ALL_JAVA_OPTS} -Dcom.sun.management.jmxremote.authenticate=false"
fi

if [ ${VISUAL_VM_TITLE} ]; then
    ALL_JAVA_OPTS="${ALL_JAVA_OPTS} -Dvisualvm.display.name=${VISUAL_VM_TITLE}"
fi

# For Cygwin -> Windows format
if ${cygwin}; then
    [ -n "$JAVA_HOME" ] && JAVA_HOME=`cygpath -w "$JAVA_HOME"`
    [ -n "$HEKATE_HOME" ] && HEKATE_HOME=`cygpath -w "$HEKATE_HOME"`
    [ -n "$CLASSPATH" ] && CLASSPATH=`cygpath -pw "$CLASSPATH"`
fi

#----- Execute Command. ------------------
have_tty=0
if [ "`tty`" != "not a tty" ]; then
    have_tty=1
fi

if [ ${have_tty} -eq 1 ]; then
    echo "=========================================="
    echo "Using HEKATE_HOME:     ${HEKATE_HOME}"
    echo "Using JAVA:            ${JAVA_EXEC}"
    echo "Using JAVA_OPTS:      ${ALL_JAVA_OPTS}"
    echo "Using CLASSPATH:       ${CLASSPATH}"
    echo "------------------------------------------"

    # Print Java version.
    javaVer=`${JAVA_EXEC} -version 2>&1`

    echo "$javaVer"

    echo "=========================================="
fi

#Execute Java with the applicable properties
"${JAVA_EXEC}" ${ALL_JAVA_OPTS} -Dhekate.home="${HEKATE_HOME}" -classpath "${CLASSPATH}" io.hekate.runner.HekateRunner $@
