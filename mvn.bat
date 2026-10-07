@echo off
set "IDEA_MAVEN=C:\Program Files\JetBrains\IntelliJ IDEA 2025.3.2\plugins\maven\lib\maven3\bin\mvn.cmd"
if exist "%IDEA_MAVEN%" (
    "%IDEA_MAVEN%" %*
) else (
    mvn %*
)
