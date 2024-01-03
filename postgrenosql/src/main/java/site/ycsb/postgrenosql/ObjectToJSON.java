package site.ycsb.postgrenosql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.util.ArrayList;
import java.util.List;
/**
 * PostgreNoSQL client for YCSB framework.
 */
public class ObjectToJSON {

  public ObjectToJSON() {
  }

  public String objectToJson() {
    Day monday = new Day();
    monday.setName("Monday");
    monday.setClose("20:00");
    monday.setOpen("18:00");
    Day thuesday = new Day();
    thuesday.setClose("20:00");
    thuesday.setOpen("18:00");
    Day wednsday = new Day();
    wednsday.setClose("20:00");
    wednsday.setOpen("18:00");
    Day thirstday = new Day();
    thirstday.setClose("20:00");
    thirstday.setOpen("18:00");
    Day friday = new Day();
    friday.setClose("20:00");
    friday.setOpen("18:00");
    Day samday = new Day();
    samday.setClose("20:00");
    samday.setOpen("18:00");
    Day sunday = new Day();
    sunday.setClose("20:00");
    sunday.setOpen("18:00");

    Hours hours = new Hours();
    List<Day> days = new ArrayList<>();
    hours.setDays(days);

    days.add(monday);
    days.add(thuesday);
    days.add(wednsday);
    days.add(thirstday);
    days.add(friday);
    days.add(samday);
    days.add(sunday);

    Attribs attribs = new Attribs();
    attribs.setTakeout(true);
    attribs.setWifi(false);

    GoodFor goodFor = new GoodFor();
    goodFor.setBreakfast(true);
    goodFor.setBrunch(true);
    goodFor.setDessert(false);
    goodFor.setDinner(false);
    goodFor.setLatenight(true);
    goodFor.setLunch(true);

    attribs.setGoodFor(goodFor);

    Document doc = new Document();

    doc.setAttributes(attribs);
    doc.setCity("Phoenix");
    doc.setState("AZ");
    doc.setName("Holly Molly");
    doc.setFullAddress("Phoenix, 35th street, 93220, AZ");
    doc.setHours(hours);
    doc.setStars(4);


    String json;
    ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
    try {
      json = ow.writeValueAsString(doc);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }




    return json;

  }



}
