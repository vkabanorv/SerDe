public class Transaction {

    //{"PLASTIC_ID":96349208, "UDATE":20200729, "TIME":132725, "ATMID":"99999999", "TERMINAL_TYPE":8}
    public int PLASTIC_ID;
    Transaction(){ }
    public int getPlasticId() {
        return this.PLASTIC_ID;
    }

    public void setPlasticId(int plasticId) {
        this.PLASTIC_ID=plasticId;
    }
}
