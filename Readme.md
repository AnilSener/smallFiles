1. Install gradle 2.12
2. Install last version of groovy
3. Install NIFI (Tested in version 0.8)
4. build nifi-client project /opt/nifi-client
5. run "gradle shadowJar it will create a jar at build/libs/nifi-client-<version>-all.jar that you can put on a Java
or Groovy classpath:  groovy -cp build/libs/nifi-client-0.3-all.jar yourScript.groovy
6. Fill data collection.properties according to your preferences
