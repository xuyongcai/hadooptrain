package com.imooc.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

/**
 * Hadoop HDFS Java API 操作
 * @author : xiaochai
 * @since : 2018/9/7
 */
public class HDFSApp {

    /**
     * 请先在C:\Windows\System32\drivers\etc\hosts文件下设置“xiaochai”对应的服务器外网ip
     */
    public static final String HDFS_PATH = "hdfs://xiaochai:9000";

    FileSystem fileSystem = null;
    Configuration configuration = null;

    @Before
    public void setUp() throws Exception {
        System.out.println("HDFSApp.setUp");
        configuration = new Configuration();
//        configuration.set("dfs.replication", "1"); // 设置hdfs副本，不设置默认3
        configuration.set("dfs.client.use.datanode.hostname", "true");//让可以使用主机名传参数

        configuration.set("fs.hdfs.impl","org.apache.hadoop.hdfs.DistributedFileSystem");
        /**
         * 对于单节点安装,只有一个datanode，默认replace-datanode-on-failure.policy是DEFAULT,
         * 如果系统中的datanode大于等于1，它会找另外一个datanode来拷贝。目前机器只有1台，因此只要一台
         * datanode出问题，就一直无法写入成功。需要配置出现此种情况应该采取的策略"NEVER"
         */
        configuration.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");

        fileSystem = FileSystem.get(new URI(HDFS_PATH),configuration,"hadoop");
    }
    @After
    public void tearDown() throws Exception{
        configuration = null;
        fileSystem = null;
        System.out.println("HDFSApp.tearDown");
    }

    /**
     * 创建hdfs目录
     * @throws Exception
     */
    @Test
    public void mkdir() throws Exception{
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    /**
     * 创建文件
     * @throws Exception
     */
    @Test
    public void create() throws Exception{
        FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
        output.write("hello hadoop".getBytes());
        output.flush();
        output.close();
    }

    /**
     * 查看HDFS文件的内容
     * @throws Exception
     */
    @Test
    public void cat() throws Exception{
        FSDataInputStream input = fileSystem.open(new Path("/hdfsapi/test/a.txt"));
        IOUtils.copyBytes(input, System.out, 1024);
        input.close();
    }

    /**
     * 重命名
     */
    @Test
    public void rename() throws Exception {
        Path oldPath = new Path("/hdfsapi/test/a.txt");
        Path newPath = new Path("/hdfsapi/test/b.txt");
        fileSystem.rename(oldPath,newPath);
    }

    /**
     * 上传文件到HDFS
     * （windows环境下运行Hadoop，会出现文件上传下载访问问题，解决方法请打开下面连接：
     * https://download.csdn.net/download/weixin_42427364/10696489）
     *
     * @throws Exception
     */
    @Test
    public void copyFromLocalFile() throws Exception {
        Path localPath = new Path("C:\\Users\\passion\\Desktop\\a.txt");
        Path hdfsPath = new Path("/hdfsapi/test");
        fileSystem.copyFromLocalFile(localPath,hdfsPath);
    }

    /**
     * 上传文件到HDFS
     */
    @Test
    public void copyFromLocalFileWithProgress() throws Exception {
        InputStream in = new BufferedInputStream(
                new FileInputStream(
                        new File("C:\\Users\\passion\\Downloads\\natapp_linux_amd64_2_3_8.zip")));

         FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/natapp_linux_amd64_2_3_8.zip"),
                new Progressable() {
                    public void progress() {
                        System.out.println(".");
                    }
                });

        IOUtils.copyBytes(in, output,4096);
    }

    /**
     * 下载HDFS文件
     */
    @Test
    public void copyToLocalFile() throws Exception {
        Path localPath = new Path("C:\\Users\\passion\\Desktop\\a.txt");
        Path hdfsPath = new Path("/hdfsapi/test/b.txt");
        fileSystem.copyToLocalFile(hdfsPath,localPath);
    }

    /**
     * 查看某个目录下的所有文件
     */
    @Test
    public void listFiles() throws Exception {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));

        for (FileStatus fileStatus : fileStatuses){
            String isDir = fileStatus.isDirectory()? "文件夹" : "文件";
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();

            System.out.println(isDir + "\t" + replication + "\t" + len + "\t" + path);
        }
    }

    /**
     * 删除
     */
    @Test
    public void delete() throws Exception{
        fileSystem.delete(new Path("/hdfsapi/test/a.txt") ,true);
    }

}

