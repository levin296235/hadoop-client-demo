package com.gaozw.test.hadoop;
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FSDataOutputStream;  
import org.apache.hadoop.fs.FileStatus;  
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IOUtils;  
import java.io.InputStream;  
import java.net.URI;  
public class TestHadoop {
	public static void main(String[] args) throws Exception {
		String uri = "hdfs://192.168.160.130:9000/";  
        Configuration config = new Configuration();  
        System.setProperty("hadoop.home.dir", "D:/myPlugins/hadoop-2.7.3/"); 
        System.setProperty("HADOOP_USER_NAME", "hadoop"); 
        FileSystem fs = FileSystem.get(URI.create(uri), config);  
   
        // 列出hdfs上/user/fkong/目录下的所有文件和目录  
        FileStatus[] statuses = fs.listStatus(new Path("/"));  
        for (FileStatus status : statuses) {  
            System.out.println(status);  
        }  
   
        // 在hdfs的/user/fkong目录下创建一个文件，并写入一行文本  
        FSDataOutputStream os = fs.create(new Path("/test/output/out.txt"));  
        os.write("welcome to you ~".getBytes());  
        os.flush();  
        os.close();  
   
        // 显示在hdfs的/user/fkong下指定文件的内容  
        InputStream is = fs.open(new Path("/test/output/out.txt"));  
        IOUtils.copyBytes(is, System.out, 1024, true);  
	}
}
