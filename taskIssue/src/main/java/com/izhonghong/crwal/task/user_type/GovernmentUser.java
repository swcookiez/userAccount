package com.izhonghong.crwal.task.user_type;

/**
 * @author sw
 * @deploy
 */

public class GovernmentUser extends CollectType {
    @Override
    public void run() {
        sendData("isGovernment");
    }
}
