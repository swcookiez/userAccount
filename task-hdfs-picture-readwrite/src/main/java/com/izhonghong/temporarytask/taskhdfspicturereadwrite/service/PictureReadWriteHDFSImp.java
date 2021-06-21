package com.izhonghong.temporarytask.taskhdfspicturereadwrite.service;

import com.izhonghong.temporarytask.taskhdfspicturereadwrite.common.HDFSConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Base64;

@Service
public class PictureReadWriteHDFSImp implements PictureReadWriteHDFS{
    //规定图片HDFS保存的路径

    static FileSystem fs;
    static final Base64.Decoder DECODER = Base64.getDecoder();
    static final Base64.Encoder ENCODER = Base64.getEncoder();

    @Override
    public String pictureWriteHDFS(String BASE64Str, String fileName) {
        String returnMessage = "";
        byte[] decode = DECODER.decode(BASE64Str);
        //获取HDFS文件系统
        initConfiguration();
        //获取输出流
        FSDataOutputStream fos = null;
        try {
            fos = fs.create(new Path(HDFSConstant.HDFS_PICTURE_PATH+fileName),false);
            fos.write(decode);
            fos.flush();
            returnMessage = "写入成功！";
        } catch (IOException e) {
            e.printStackTrace();
            returnMessage = "写入失败,请检查文件名是否已存在，或检查base64格式是否正确";
        }finally {
            if (fos!=null){
                try {
                    fos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return returnMessage;
    }

    @Override
    public String pictureReadHDFS(String fileName) {
        String pictureBase64Str = "";
        initConfiguration();
        // 2 获取输入流
        FSDataInputStream fis = null ;
        try {
            fis = fs.open(new Path(HDFSConstant.HDFS_PICTURE_PATH+fileName));
            // 创建接收字节数组
            byte[] bytes = new byte[fis.available()];
            fis.readFully(bytes);
            pictureBase64Str = ENCODER.encodeToString(bytes);
        } catch (IOException e) {
            e.printStackTrace();
            pictureBase64Str = "查询图片不存在,请检查文件名";
        }finally {
            if (fis!=null){
                try {
                    fis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return pictureBase64Str;
    }

    //hdfs文件系统配置，获取hdfs文件系统
    public static void initConfiguration(){
        if (fs==null){
            // 1 获取文件系统
            Configuration configuration = new Configuration();
            // 设置文件的副本数也可以不设置
            configuration.set("dfs.replication", "2");
            try {
                //hadoop地址，用户名
                fs = FileSystem.get(new URI(HDFSConstant.HDFS_ADDRESS), configuration, HDFSConstant.HDFS_USER);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
        }
    }
}
