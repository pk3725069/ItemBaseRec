package itemBasedRec.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class RecSort implements  WritableComparable<RecSort>{
	private int userID;
    private int itemID;
    private double sumPref;
	public int getItemID() {
		return itemID;
	}
	public void setItemID(int itemID) {
		this.itemID = itemID;
	}
	public double getSumPref() {
		return sumPref;
	}
	public void setSumPref(double sumPref) {
		this.sumPref = sumPref;
	}
	public RecSort(int userID,int itemID,double sumPref)
	{
		this.userID = userID;
		this.itemID = itemID;
		this.sumPref = sumPref;
	}
	public int getUserID() {
		return userID;
	}
	public void setUserID(int userID) {
		this.userID = userID;
	}
	//key的比较
    public int compareTo(RecSort o)
    {
        // TODO Auto-generated method stub
        if (userID != o.userID)
        {
            return userID < o.userID ? -1 : 1;
        }
        else if (sumPref != o.sumPref)
        {
            return sumPref > o.sumPref ? -1 : 1;
        }
        else
        {
            return 0;
        }
    }
	//在反序列化时，反射机制需要调用空参构造函数，所以显示定义了一个空参构造函数
	public RecSort(){}
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(userID);
		out.writeInt(itemID);
		out.writeDouble(sumPref);
	}
	public void readFields(DataInput in) throws IOException {
		userID = in.readInt();
		itemID = in.readInt();
		sumPref = in.readDouble();
		
	}

}