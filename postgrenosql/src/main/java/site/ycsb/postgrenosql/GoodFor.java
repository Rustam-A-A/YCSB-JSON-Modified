package site.ycsb.postgrenosql;

/**
 * PostgreNoSQL client for YCSB framework.
 */
public class GoodFor {
  private boolean dessert;
  private boolean latenight;
  private boolean lunch;
  private boolean dinner;
  private boolean brunch;
  private boolean breakfast;


  public GoodFor() {
  }

  public boolean isDessert() {
    return dessert;
  }

  public void setDessert(boolean dessert1) {
    this.dessert = dessert1;
  }

  public boolean isLatenight() {
    return latenight;
  }

  public void setLatenight(boolean latenight1) {
    this.latenight = latenight1;
  }

  public boolean isLunch() {
    return lunch;
  }

  public void setLunch(boolean lunch1) {
    this.lunch = lunch1;
  }

  public boolean isDinner() {
    return dinner;
  }

  public void setDinner(boolean dinner1) {
    this.dinner = dinner1;
  }

  public boolean isBrunch() {
    return brunch;
  }

  public void setBrunch(boolean brunch1) {
    this.brunch = brunch1;
  }

  public boolean isBreakfast() {
    return breakfast;
  }

  public void setBreakfast(boolean breakfast1) {
    this.breakfast = breakfast1;
  }
}
