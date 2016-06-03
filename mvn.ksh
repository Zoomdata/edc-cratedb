

#export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.7.0_67.jdk/Contents/Home
#export PATH=$JAVA_HOME/bin:$PATH
java -version
mvn clean install -Dmaven.test.skip=true

