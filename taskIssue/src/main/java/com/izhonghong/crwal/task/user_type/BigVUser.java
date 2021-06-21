package com.izhonghong.crwal.task.user_type;


/**
 * @author sw
 * @deploy
 */

public class BigVUser extends CollectType {
    @Override
    public void run() {
      sendData("IsBigV"); //todo 优化成isBigV
    }
}
