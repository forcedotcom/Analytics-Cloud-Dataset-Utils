@ECHO OFF
SET BASEDIR=%~dp0.
cd %BASEDIR%

FOR /F "delims=|" %%I IN ('DIR /B /O:D') DO SET LATEST_JAR=%%I
echo %LATEST_JAR%

java -cp .;%LATEST_JAR% com.sforce.dataset.util.UpgradeChecker

FOR /F "delims=|" %%I IN ('DIR /B /O:D') DO SET LATEST_JAR=%%I
echo %LATEST_JAR%

java -Xmx1G -jar %LATEST_JAR% --server true
