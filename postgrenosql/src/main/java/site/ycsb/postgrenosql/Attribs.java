package site.ycsb.postgrenosql;


/**
 * PostgreNoSQL client for YCSB framework.
 */
public class Attribs {
  private boolean takeout;
  private boolean wifi;
  private GoodFor goodfor;

  public Attribs() {
  }

  public Attribs(boolean takeout, boolean wifi, GoodFor goodfor) {
    this.takeout = takeout;
    this.wifi = wifi;
    this.goodfor = goodfor;
  }

  public boolean isTakeout() {
    return takeout;
  }

  public void setTakeout(boolean takeout1) {
    this.takeout = takeout1;
  }

  public boolean getWifi() {
    return wifi;
  }

  public void setWifi(boolean wifi1) {
    this.wifi = wifi1;
  }

  public GoodFor getGoodfor() {
    return goodfor;
  }


  public void setGoodFor(GoodFor goodfor1) {
    this.goodfor = goodfor1;
  }

}
