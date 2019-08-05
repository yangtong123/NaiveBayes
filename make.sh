#!/bin/bash

#判断shell脚本的命令行参数是否指定src目录
if [ -n "$1" ];then
    SRC_DIR=$1;
else
    SRC_DIR=$(pwd)/src
fi

#得到src目录的完整路径
CURRENT_DIR=$(pwd)
cd $SRC_DIR
SRC_DIR=$(pwd)
cd $CURRENT_DIR


#判断指定的src目录是否存在
if [ ! -d $SRC_DIR ];then
    echo $SRC_DIR"目录不存在"
    exit 1
fi

echo 源文件目录:$SRC_DIR


#判断是否设置JAVA_HOME环境变量
if [ ! -n "$JAVA_HOME" ];then
    echo "没有设置JAVA_HOME环境变量"
    exit 1
fi;

#判断是否设置HADOOP_HOME环境变量
if [ ! -n "$HADOOP_HOME" ];then
    echo "没有设置HADOOP_HOME环境变量"
    exit 1
fi;

#判断是否设置HADOOP_HOME环境变量
if [ ! -n "$SPARK_HOME" ];then
    echo "没有设置SPARK_HOME环境变量"
    exit 1
fi;

#如果存在$SRC_DIR/../bin目录，则首先删除，再创建
if [ -d $SRC_DIR/../bin ];then
    rm -R $SRC_DIR/../bin
fi
mkdir $SRC_DIR/../bin

#如果存在$SRC_DIR/../temp目录，则首先删除，再创建
if [ -d $SRC_DIR/../temp ];then
    rm -R $SRC_DIR/../temp
fi
mkdir $SRC_DIR/../temp

#递归地找出src目录下所有的java文件，并拷贝到$SRC_DIR/../temp目录下
for file in $(find $SRC_DIR |grep '\.java$')
do
#   echo $file
    cp $file $SRC_DIR/../temp
done

#设置所需的JavaJDK Jar包
JDKCLASSPATH=
for jarfile in $(find $JAVA_HOME | grep '\.jar$')
do
#   echo $jarfile
    if [ ! -n "$JDKCLASSPATH" ];then
        JDKCLASSPATH=$jarfile
    else
        JDKCLASSPATH=$JDKCLASSPATH:$jarfile
    fi
done
#echo $JDKCLASSPATH

#设置编译所需要的Hadoop Jar包
HADOOP_JARS=
for jarfile in $(find $HADOOP_HOME | grep '\.jar$')
do
    if [ ! -n "$HADOOP_JARS" ];then
        HADOOP_JARS=$jarfile
    else
        HADOOP_JARS=$HADOOP_JARS:$jarfile
    fi
done
#echo $HADOOP_JARS

#设置编译所需要的Spark Jar包
SPARK_JARS=
for jarfile in $(find $SPARK_HOME | grep '\.jar$')
do
    if [ ! -n "$SPARK_JARS" ];then
        SPARK_JARS=$jarfile
    else
        SPARK_JARS=$SPARK_JARS:$jarfile
    fi
done
#echo $SPARK_JARS

#设置编译时需要的HADOOP_CLASSPATH
HADOOP_CLASSPATH=$SRC_DIR/../bin


#设置相关的环境变量
#export JAVA_HOME=$JAVA_HOME
#export PATH=$PATH:$JAVA_HOME/bin
#export HADOOP_JARS
#export HADOOP_CLASSPATH

#编译$SRC_DIR/../temp下所有的java文件，输出的.class文件放到$SRC_DIR/../bin目录下
echo 开始编译
javac $SRC_DIR/../temp/*.java -d $SRC_DIR/../bin -encoding UTF-8 -classpath $JDKCLASSPATH:$HADOOP_JARS:$SPARK_JARS:$HADOOP_CLASSPATH

#删除temp目录
if [ -d $SRC_DIR/../temp ];then
    rm -R $SRC_DIR/../temp
fi
echo 

#递归遍历src目录，将所有非.java文件（如xml配置文件）拷贝到bin目录下
cd $SRC_DIR
for item in $(find . | grep -v \.java$)
do
    if [ -f $item ];then
#       echo $item
        cp --parents $item $SRC_DIR/../bin
    fi
done
cd $CURRENT_DIR

#打包成Jar文件
jar -cvf run.jar -C $SRC_DIR/../bin .


echo 编译完成
