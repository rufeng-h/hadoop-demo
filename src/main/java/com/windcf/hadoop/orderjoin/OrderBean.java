package com.windcf.hadoop.orderjoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author chunf
 * @time 2022-10-13 8:49
 * @package com.windcf.hadoop.orderjoin
 * @description TODO
 */
public class OrderBean implements Writable {
    private final LongWritable orderId = new LongWritable();
    private final Text prodId = new Text();
    private final LongWritable amount = new LongWritable();
    private final Text prodName = new Text();

    public OrderBean(long orderId, String prodId, long amount, String prodName) {
        this.orderId.set(orderId);
        this.prodId.set(prodId);
        this.amount.set(amount);
        this.prodName.set(prodName);
    }

    public LongWritable getOrderId() {
        return orderId;
    }

    public Text getProdId() {
        return prodId;
    }

    public LongWritable getAmount() {
        return amount;
    }

    public Text getProdName() {
        return prodName;
    }

    public OrderBean() {

    }

    @Override
    public void write(DataOutput out) throws IOException {
        orderId.write(out);
        prodId.write(out);
        amount.write(out);
        prodName.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        orderId.readFields(in);
        prodId.readFields(in);
        amount.readFields(in);
        prodName.readFields(in);
    }

}
