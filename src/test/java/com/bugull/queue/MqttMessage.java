package com.bugull.queue;

public class MqttMessage {
    private long time;
    private int type;
    private String content;
    private long id;

    public MqttMessage() {
    }

    public MqttMessage(long time, int type, String content, long id) {
        this.time = time;
        this.type = type;
        this.content = content;
        this.id = id;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "MqttMessage{" +
                "time=" + time +
                ", type=" + type +
                ", content='" + content + '\'' +
                ", id=" + id +
                '}';
    }
}
