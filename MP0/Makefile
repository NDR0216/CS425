all: logger.class node.class

path := ../src

logger.class: logger.java
	javac --release 8 logger.java

node.class: node.java
	javac --release 8 node.java

logger.java: $(path)/logger.java
	cp $(path)/logger.java logger.java

node.java: $(path)/node.java
	cp $(path)/node.java node.java