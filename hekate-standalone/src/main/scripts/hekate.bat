@echo off
setlocal
::***************************************************************************
:: Java configuration options.
::***************************************************************************

::-------------------------------------------------
:: JVM memory options (-Xms and -Xmx)
::-------------------------------------------------
:: set JAVA_MIN_MEM=128m
:: set JAVA_MAX_MEM=512m

::-------------------------------------------------
:: Additional JVM options
::-------------------------------------------------
set JAVA_OPTS=-server

::-------------------------------------------------
:: JMX remote access (non secure)
::-------------------------------------------------
:: set JMX_PORT=9010

::-------------------------------------------------
:: Application title in VisualVM (no whitespaces)
::-------------------------------------------------
:: set VISUAL_VM_TITLE=

::***************************************************************************
:: Startup script.
::***************************************************************************
::----- Resolve HEKATE_HOME. ----------------------
if not "%HEKATE_HOME%" == "" goto gotHome
    :: Auto-detect.
    pushd "%~dp0"/..
        set HEKATE_HOME=%CD%
    popd

:cleanupHome
:: Strip double quotes
set HEKATE_HOME=%HEKATE_HOME:"=%

:: Remove trailing slashes.
if %HEKATE_HOME:~-1,1% == \ goto cleanupHomeLoop
if %HEKATE_HOME:~-1,1% == / goto cleanupHomeLoop
goto gotHome

:cleanupHomeLoop
set HEKATE_HOME=%HEKATE_HOME:~0,-1%
goto cleanupHome


:gotHome
if exist "%HEKATE_HOME%\bin\hekate.bat" goto okHome
    echo The HEKATE_HOME environment variable is not defined correctly: %HEKATE_HOME%
    goto end

:okHome
::----- Resolve JAVA_HOME. ------------------------
if "%JAVA_HOME%" == "" goto noJavaHome
if not exist "%JAVA_HOME%\bin\java.exe" goto noJavaHome
goto okJava

:noJavaHome
echo The JAVA_HOME environment variable is not defined correctly.
goto exit

:okJava
set JAVA_EXEC=%JAVA_HOME%\bin\java.exe

::----- Resolve CLASSPATH. ------------------------
set CP_LIB=%HEKATE_HOME%\lib\*

for /D %%F in (%HEKATE_HOME%\lib\*) do call :concat %%F\*

set CLASSPATH=%CP_LIB%;%HEKATE_HOME%/config;

::----- Resolve JVM options. ------------------
if not "%JAVA_MIN_MEM%" == "" goto jvmMinMaxSameDone
  set JAVA_MIN_MEM=%JAVA_MAX_MEM%
:jvmMinMaxSameDone


if "%JAVA_MIN_MEM%" == "" goto jvmMinMemDone
  set ALL_JAVA_OPTS=%ALL_JAVA_OPTS% -Xms%JAVA_MIN_MEM%
:jvmMinMemDone

if "%JAVA_MAX_MEM%" == "" goto jvmMaxMemDone
  set ALL_JAVA_OPTS=%ALL_JAVA_OPTS% -Xmx%JAVA_MAX_MEM%
:jvmMaxMemDone

set ALL_JAVA_OPTS=%ALL_JAVA_OPTS% %JAVA_OPTS%

if "%JMX_PORT%" == "" goto jmxDone
  set ALL_JAVA_OPTS=%ALL_JAVA_OPTS% -Dcom.sun.management.jmxremote.port=%JMX_PORT%
  set ALL_JAVA_OPTS=%ALL_JAVA_OPTS% -Dcom.sun.management.jmxremote
  set ALL_JAVA_OPTS=%ALL_JAVA_OPTS% -Dcom.sun.management.jmxremote.local.only=false
  set ALL_JAVA_OPTS=%ALL_JAVA_OPTS% -Dcom.sun.management.jmxremote.authenticate=false
:jmxDone

if "%VISUAL_VM_TITLE%" == "" goto vvmDone
  set ALL_JAVA_OPTS=%ALL_JAVA_OPTS% -Dvisualvm.display.name="%VISUAL_VM_TITLE%"
:vvmDone

::----- Execute Command. ------------------
echo ==========================================
echo Using HEKATE_HOME:     %HEKATE_HOME%
echo Using JAVA:            %JAVA_EXEC%
echo Using JAVA_OPTS:      %ALL_JAVA_OPTS%
echo Using CLASSPATH:       %CLASSPATH%
echo ------------------------------------------

:: Print Java version.
for /f "tokens=*" %%V in ('%JAVA_EXEC% -version 2^>^&1') do echo %%V

echo ==========================================

::Execute Java with the applicable properties
%JAVA_EXEC% %ALL_JAVA_OPTS% -classpath "%CLASSPATH%" -Dhekate.home="%HEKATE_HOME%" io.hekate.runner.HekateRunner %*

:concat
    set CP_LIB=%CP_LIB%;%1
goto :end

:end
