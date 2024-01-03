package site.ycsb.postgrenosql;

/**
 * PostgreNoSQL client for YCSB framework.
 */
public class Document {
  private String fullAddress;
  private Hours hours;
  private String city;
  private int reviewCount;
  private String name;
  private String state;
  private int stars;
  private Attribs attributes;

  public Document() {
  }

  public String getFullAddress() {
    return fullAddress;
  }

  public void setFullAddress(String fullAddress1) {
    this.fullAddress = fullAddress1;
  }

  public Hours getHours() {
    return hours;
  }

  public void setHours(Hours hours1) {
    this.hours = hours1;
  }

  public String getCity() {
    return city;
  }

  public void setCity(String city1) {
    this.city = city1;
  }

  public int getReviewCount() {
    return reviewCount;
  }

  public void setReviewCount(int reviewCount1) {
    this.reviewCount = reviewCount1;
  }

  public String getName() {
    return name;
  }

  public void setName(String name1) {
    this.name = name1;
  }

  public String getState() {
    return state;
  }

  public void setState(String state1) {
    this.state = state1;
  }

  public int getStars() {
    return stars;
  }

  public void setStars(int stars1) {
    this.stars = stars1;
  }

  public Attribs getAttributes() {
    return attributes;
  }

  public void setAttributes(Attribs attributes1) {
    this.attributes = attributes1;
  }

}



