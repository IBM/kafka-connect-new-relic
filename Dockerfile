FROM confluentinc/cp-kafka-connect:5.2.3

# Copy JDBC plugins
COPY ./build/libs/*.jar /usr/share/java/kafka-connect-jdbc/
