package com.windcf.hadoop.bean;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * @author chunf
 * @time 2022-10-11 18:43
 * @package com.windcf.hadoop.bean
 * @description TODO
 */
public class PhoneFlowBean implements WritableComparable<PhoneFlowBean> {
    private Text phone;
    private LongWritable upFlow;
    private LongWritable downFlow;
    private LongWritable sumFlow;

    public PhoneFlowBean() {
        this.phone = new Text();
        this.downFlow = new LongWritable();
        this.upFlow = new LongWritable();
    }

    public PhoneFlowBean(String phone, long upFlow, long downFlow) {
        this.phone = new Text(phone);
        this.upFlow = new LongWritable(upFlow);
        this.downFlow = new LongWritable(downFlow);
        this.sumFlow = new LongWritable(upFlow + downFlow);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        phone.write(out);
        upFlow.write(out);
        downFlow.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        phone.readFields(in);
        upFlow.readFields(in);
        downFlow.readFields(in);
        this.sumFlow = new LongWritable(upFlow.get() + downFlow.get());
    }

    public Text getPhone() {
        return phone;
    }

    public void setPhone(Text phone) {
        this.phone = phone;
    }

    public LongWritable getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(LongWritable upFlow) {
        this.upFlow = upFlow;
    }

    public LongWritable getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(LongWritable downFlow) {
        this.downFlow = downFlow;
    }

    public LongWritable getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(LongWritable sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public String toString() {
        return new StringJoiner("\t")
                .add(upFlow.toString())
                .add(downFlow.toString())
                .add(sumFlow.toString())
                .toString();
    }

    @Override
    public int compareTo(PhoneFlowBean o) {
        int i = this.sumFlow.compareTo(o.sumFlow);
        return i != 0 ? i : this.upFlow.compareTo(o.upFlow);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PhoneFlowBean)) {
            return false;
        }
        PhoneFlowBean phoneFlowBean = (PhoneFlowBean) o;
        return phone.equals(phoneFlowBean.phone) && upFlow.equals(phoneFlowBean.upFlow) && downFlow.equals(phoneFlowBean.downFlow);
    }

    @Override
    public int hashCode() {
        return Objects.hash(phone, upFlow, downFlow);
    }
}
