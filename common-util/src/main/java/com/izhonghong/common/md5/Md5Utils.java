package com.izhonghong.common.md5;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * MD5工具类
 * 2016-02-23 有重大更新！MessageDigest非线程安全，此前的版本直接在方法中增加了同步关键字sycchronized
 * 现在修改为ThreadLocal的方式
 */
public class Md5Utils {
    //匿名实现类对象
    private static ThreadLocal<MessageDigest> threadLocal = new ThreadLocal<MessageDigest>(){
        //局部变量初始值方法，返回的是该线程局部变量的值。
        //此方法延迟调用，且只执行一次。调用get、set才执行
        @Override
        protected MessageDigest initialValue() {
            try {
                return MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            return null;
        }
    };

    /*
     *  MessageDigest类用于为应用程序提供信息摘要算法的功能.   提供信息摘要
     *  简单来说是用于生成散列码。具体来讲  source：任意数据   transform：自定义  sink：定长 hashcode
     */
    public static MessageDigest getMD() {
        return threadLocal.get();
    }

	/*private static MessageDigest md = null;
	static{
		try {
			md = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
		}
	}*/
    private final static String[] charDigits = { "0", "1", "2", "3", "4", "5",
            "6", "7", "8", "9", "a", "b", "c", "d", "e", "f" };

    /*
        依据字节返回字符 =》 加密
     */
    private static String byteToArrayString(byte bByte) {
        int iRet = bByte;
        if (iRet < 0) {
            iRet += 256;
        }
        int iD1 = iRet / 16;
        int iD2 = iRet % 16;
        return charDigits[iD1] + charDigits[iD2];
    }

    /**
     * 转换字节数组为16进制字串
     * @param bytes
     * @return
     */
    private static String byteToString(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bytes.length; i++) {
            sb.append(byteToArrayString(bytes[i]));
        }
        return sb.toString();
    }

    /*
        此方法返回了加密后的字符串
     */
    public static String getMd5ByStr(String str) {
            //return  byteToString(md.digest(str.getBytes()));
            try {
                // getMessageDigest  ： 输入：字节数组  输出：字节数组
				return  byteToString(getMD().digest(str.getBytes("utf8")));
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
    }
    public static String getMd5ByBytes(byte[] bytes) {
        //return  byteToString(md.digest(bytes));
        return  byteToString(getMD().digest(bytes));
    }
    public static void main(String[] args) {
        /*String md5ByStr = getMd5ByStr("徐沟沟").substring(7,23);
        System.out.println(md5ByStr);*/
        System.out.println(getMd5ByStr("6624421775"));
    }
}














