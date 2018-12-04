package com.xuyongcai.hadoop.chapter05;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author: xiaochai
 * @create: 2018-11-29
 **/
public class MemberLogTime implements WritableComparable<MemberLogTime> {

    private String member_name;
    private String logTime;

    public MemberLogTime() {
    }

    public MemberLogTime(String member_name, String logTime) {
        this.member_name = member_name;
        logTime = logTime;
    }

    public String getMember_name() {
        return member_name;
    }

    public void setMember_name(String member_name) {
        this.member_name = member_name;
    }

    public String getLogTime() {
        return logTime;
    }

    public void setLogTime(String logTime) {
        logTime = logTime;
    }


    public int compareTo(MemberLogTime o) {
        return this.getMember_name().compareTo(o.getMember_name());
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(member_name);
        dataOutput.writeUTF(logTime);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.member_name = dataInput.readUTF();
        this.logTime = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return this.member_name + "," + this.logTime;
    }
}
