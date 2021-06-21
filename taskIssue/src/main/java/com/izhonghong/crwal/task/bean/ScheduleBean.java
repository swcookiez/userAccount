package com.izhonghong.crwal.task.bean;

import com.izhonghong.crwal.task.user_type.CollectType;

import java.util.Timer;

/**
 * @author sw
 * @deploy
 */
public class ScheduleBean {
    private Long period;
    private CollectType type;
    private Timer timer;

    public ScheduleBean() {
    }

    public ScheduleBean(Long period, CollectType type, Timer timer) {
        this.period = period;
        this.type = type;
        this.timer = timer;
    }

    public Long getPeriod() {
        return period;
    }

    public CollectType getType() {
        return type;
    }

    public Timer getTimer() {
        return timer;
    }

    public void setPeriod(Long period) {
        this.period = period;
    }

    public void setType(CollectType type) {
        this.type = type;
    }

    public void setTimer(Timer timer) {
        this.timer = timer;
    }
}
