package com.izhonghong.crwal.task.user_type;

/**
 * @author sw
 * @deploy
 */

public class MediaUser extends CollectType {
    @Override
    public void run() {
        sendData("isMedia");
    }
}
