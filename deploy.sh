export MAVEN_DEPLOY_OPTIONS="--add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.desktop/java.awt.font=ALL-UNNAMED"
export MAVEN_OPTS="$MAVEN_DEPLOY_OPTIONS"
export GPG_TTY=$(tty)

mvn clean deploy $@
