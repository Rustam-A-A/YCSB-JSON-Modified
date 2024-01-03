package site.ycsb.postgrenosql;

import java.util.List;


/**
 * PostgreNoSQL client for YCSB framework.
 */
public class Hours {
  private List<Day> days;

  public Hours(List<Day> days1) {
    this.days = days1;
  }

  public Hours() {
  }

  public List<Day> getDays() {
    return days;
  }

  public void setDays(List<Day> days1) {
    this.days = days1;
  }
}
