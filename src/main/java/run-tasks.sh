#!/bin/bash

# Change to the codes directory
cd /app/codes

# Step 0: Delete all .class and .jar files
echo "Cleaning up previous builds..."
rm -f *.class *.jar

# Loop through each task file
for task in Task1 Task2 Task3 Task4; do
    echo "Processing $task..."
    # Step 1: Compile the Java file
    javac -cp "/opt/spark/jars/*" "$task.java"
    if [ $? -ne 0 ]; then
        echo "Compilation failed for $task"
        continue
    fi
    # Step 2: Create JAR file
    jar cf "$task.jar" *.class
    # Step 3: Submit Spark job
    /opt/spark/bin/spark-submit --master spark://localhost:7077 --class "$task" "$task.jar"
    # Clean up for next task
    rm -f *.class
done

echo "All tasks processed!"
