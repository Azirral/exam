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

TARGETS := $(addprefix run, Ex1)

.PHONY: final $(TARGETS) clean

final: $(TARGETS)

runScript1: $(OBJDIR)/Ex1.class
	jar -cvf $(JAR) -C $(OBJDIR)/ ./
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/Ex1-out/
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/Ex1-out2/
	hadoop jar $(JAR) Ex1 /user/adampap/WHV /$(HDFS)/Ex1-out /$(HDFS)/Ex1-out2 
	@echo "Run the following command to read the out file:"
	@echo "hdfs dfs -get /$(HDFS)/Ex1-out/part*"


$(OBJDIR)/%.class: %.java | $(OBJDIR)
	hadoop com.sun.tools.javac.Main $< -d $(OBJDIR)

$(OBJDIR):
	mkdir $@

.PHONY: clean
clean:
	rm -f $(OBJDIR)/* $(JAR)
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/*-out/
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/Ex1-out2/
