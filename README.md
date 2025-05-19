# CS435-Team-2
IF you decide to use my JAR file, I had all my .class files in a package called 'compiled'
This is a subdirectory located in this repository as well for reference purposes.

Here are my run commands if you require, they may need slight alterations if you have things set up differently from me:
## Compilation
``` hadoop com.sun.tools.javac.Main *.java ```

## Move the compiled .class files to the package
``` mv *.class compiled ```

## Make the jar file from the .class files
``` jar cf MapReduce.jar compiled ```

## Run command
### **Note**: This assumes that the:
###                             input path on HDFS is /group/movies.txt
###                             output path on HDFS is /groupOut

## Fetch output files to local directory for reading, assumes same output directory as above:
``` hd fs -get /groupOut/part-r-00000 ```



## To Reset for next execution:
### Remove output on HDFS
```hd fs -rm -r /groupOut```
### Remove compiled .class files
```rm compiled/*.class```
### Remove jar file
```rm MapReduce.jar```



