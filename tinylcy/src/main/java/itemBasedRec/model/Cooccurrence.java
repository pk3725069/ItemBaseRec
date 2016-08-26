package itemBasedRec.model;

public class Cooccurrence {
    private int itemID1;
    private int itemID2;
    private int num;
    private int userID;
    private double pref;
    public Cooccurrence(int itemID1, int itemID2, int num) {
        super();
        this.itemID1 = itemID1;
        this.itemID2 = itemID2;
        this.num = num;
    }
    public Cooccurrence(int itemID1, int userID, double pref) {
        super();
        this.itemID1 = itemID1;
        this.userID = userID;
        this.pref = pref;
    }
    
    public int getItemID1() {
        return itemID1;
    }

    public void setItemID1(int itemID1) {
        this.itemID1 = itemID1;
    }

    public int getItemID2() {
        return itemID2;
    }

    public void setItemID2(int itemID2) {
        this.itemID2 = itemID2;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

	public int getUserID() {
		return userID;
	}

	public void setUserID(int userID) {
		this.userID = userID;
	}

	public double getPref() {
		return pref;
	}

	public void setPref(double pref) {
		this.pref = pref;
	}

}