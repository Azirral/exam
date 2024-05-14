HADOOP_CLASSPATH := ${JAVA_HOME}/lib/tools.jar
export HADOOP_CLASSPATH

HDFS = user/bigdata34/PageRank
OBJDIR= build
JAR := PageRank.jar
TARGETS := $(addprefix run, PageRank)

.PHONY: final $(TARGETS) clean

final: $(TARGETS)

runPageRank: $(OBJDIR)/PageRank.class
	jar -cvf $(JAR) -C $(OBJDIR)/ ./
	hdfs dfs -rm -r -f -skipTrash /$(HDFS)/PageRank-out/
	hadoop jar $(JAR) PageRank -D N=4 -D K=10 -D B=1 /user/adampap/wikiSpiderTraps /$(HDFS)/PageRank-out 
	@echo "Run the following command to read the output file:"
	@echo "hdfs dfs -get /$(HDFS)/PageRank-out/Final/part*"

$(OBJDIR)/%.class: %.java | $(OBJDIR)
	hadoop com.sun.tools.javac.Main $< -d $(OBJDIR)

$(OBJDIR):
	mkdir $@

.PHONY: clean
clean:
	rm -f $(OBJDIR)/* $(JAR)
	hdfs dfs -rm -r -skipTrash -f /$(HDFS)/*-out/
