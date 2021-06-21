package com.izhonghong.crwal.task.bean;

/**
 * @author sw
 * @deploy
 */

public class TypeBean {
    private String typeName; //类型名
    private Long period; //以秒为单位

    public TypeBean() {
    }

    public TypeBean(String typeName, Long period) {
        this.typeName = typeName;
        this.period = period;
    }

    public String getTypeName() {
        return typeName;
    }

    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    public Long getPeriods() {
        return period;
    }

    public void setPeriods(Long period) {
        this.period = period;
    }

    @Override
    public String toString() {
        return
                "typeName='" + typeName + '\'' +
                ", period=" + period ;
    }
}
