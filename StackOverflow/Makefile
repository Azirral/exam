#
# This is a simple makefile to assist with quickly building the Exercises of MP2.
#
# To build and execute a particular exercise:
#    - For a single exercise, type 'make runA' to run exercise A.
#    - For all exercises, 'make'.
#
#hdfs
HADOOP_CLASSPATH := ${JAVA_HOME}/lib/tools.jar
export HADOOP_CLASSPATH

HDFS=user/bigdata34/WhiteHouse

OBJDIR=build

JAR := MapReducePSO.jar

TARGETS := $(addprefix run, DriverStackOverflow)

.PHONY: final $(TARGETS) clean

final: $(TARGETS)

runScript1: $(OBJDIR)/DriverStackOverflow.class
	jar -cvf $(JAR) -C $(OBJDIR)/ ./
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/DriverStackOverflow-out/
	hadoop jar $(JAR) DriverStackOverflow /user/adampap/stackOverflow /$(HDFS)/DriverStackOverflow-out 
	@echo "Run the following command to read the out file:"
	@echo "hdfs dfs -get /$(HDFS)/DriverStackOverflow-out/part*"


$(OBJDIR)/%.class: %.java | $(OBJDIR)
	hadoop com.sun.tools.javac.Main $< -d $(OBJDIR)

$(OBJDIR):
	mkdir $@

.PHONY: clean
clean:
	rm -f $(OBJDIR)/* $(JAR)
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/*-out/
