***NOTE:***
This module used both Java and Scala to program.
So in the pom.xml file, the Flink dependencies for Java and Scala are both included.


# How to run in IDEA
## 1.Configure IDEA to run with “Provided” dependencies
1. Click "Edit Configurations...";
2. Clink "And New Configuration";
3. In "Main Class" input box, specify your main class;
4. Under "use classpath of module" line, make "Include dependencies with "Provided" scope" selected;

## 2. Changes in the "Main" class
For Java version, in `org.jtLiBrain.flink.WindowWordCount` class, do this change:
```java
// replace
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// with
StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
```
For Scala version, in `org.jtLiBrain.flink.WindowWordCountScala` class, do this change:

```scala
// replace
val env = StreamExecutionEnvironment.getExecutionEnvironment

// with
val env = StreamExecutionEnvironment.createLocalEnvironment()
```

## 3. Run netcat first from a terminal
`bash
nc -lk 9999
`

## 4. Run the "Main" class
