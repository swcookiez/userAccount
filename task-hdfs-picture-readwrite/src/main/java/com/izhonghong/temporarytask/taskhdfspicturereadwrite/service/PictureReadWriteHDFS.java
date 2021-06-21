package com.izhonghong.temporarytask.taskhdfspicturereadwrite.service;

public interface PictureReadWriteHDFS {
    /**
     * 接收(图片)BASE64数据，将BASE64解码写入HDFS
     * @param BASE64Str BASE64字符串
     * @param fileName 文件名
     */
    String pictureWriteHDFS(String BASE64Str,String fileName);

    /**
     * 拉取HDFS中图片数据，将图片数据编码返回
     * @param fileName 文件名
     */
    String pictureReadHDFS(String fileName);
}
