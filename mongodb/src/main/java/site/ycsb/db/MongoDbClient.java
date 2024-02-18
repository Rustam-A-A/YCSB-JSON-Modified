/**
 * Copyright (c) 2012 - 2015 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/*
 * MongoDB client binding for YCSB.
 *
 * Submitted by Yen Pai on 5/11/2010.
 *
 * https://gist.github.com/000a66b8db2caf42467b#file_mongo_database.java
 */
package site.ycsb.db;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.conversions.Bson;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;


import org.bson.Document;
import org.bson.types.Binary;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MongoDB binding for YCSB framework using the MongoDB Inc. <a
 * href="http://docs.mongodb.org/ecosystem/drivers/java/">driver</a>
 * <p>
 * See the <code>README.md</code> for configuration information.
 * </p>
 * 
 * @author ypai
 * @see <a href="http://docs.mongodb.org/ecosystem/drivers/java/">MongoDB Inc.
 *      driver</a>
 */
public class MongoDbClient extends DB {

  /** Used to include a field in a response. */
  private static final Integer INCLUDE = Integer.valueOf(1);

  /** The options to use for inserting many documents. */
  private static final InsertManyOptions INSERT_UNORDERED =
      new InsertManyOptions().ordered(false);

  /** The options to use for inserting a single document. */
  private static final UpdateOptions UPDATE_WITH_UPSERT = new UpdateOptions()
      .upsert(true);

  /**
   * The database name to access.
   */
  private static String databaseName;

  /** The database name to access. */
  private static MongoDatabase database;


  /**
   * The yelp database name to access.
   */
  private static String yelpDatabaseName = "test";

  /** The database name to access. */
  private static MongoDatabase yelpDatabase;

  /** The iterator to work with original collection. */
  private static Iterator<Document> iterator;

  /** The original yelp collection name. */
  private static String yelpCollectionName = "myCollection";

  /** The original yelp collection name. */
  private static MongoCollection<Document> yelpCollection;





  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  /** A singleton Mongo instance. */
  private static MongoClient mongoClient;

  /** The default read preference for the test. */
  private static ReadPreference readPreference;

  /** The default write concern for the test. */
  private static WriteConcern writeConcern;

  /** The batch size to use for inserts. */
  private static int batchSize;

  /** If true then use updates with the upsert option for inserts. */
  private static boolean useUpsert;

  /** The bulk inserts pending for the thread. */
  private final List<Document> bulkInserts = new ArrayList<Document>();

  /** The range in direction longitude. */
  public static final double LONGITUDE_RANGE = 0.02;

  /** The range in direction latitude. */
  public static final double LATITUDE_RANGE = 0.02;

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    if (INIT_COUNT.decrementAndGet() == 0) {
      try {
        mongoClient.close();
      } catch (Exception e1) {
        System.err.println("Could not close MongoDB connection pool: "
            + e1.toString());
        e1.printStackTrace();
        return;
      } finally {
        database = null;
        mongoClient = null;
      }
    }
  }

  /**
   * Delete a record from the database.
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status delete(String table, String key) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      Document query = new Document("_id", key);
      DeleteResult result =
          collection.withWriteConcern(writeConcern).deleteOne(query);
      if (result.wasAcknowledged() && result.getDeletedCount() == 0) {
        System.err.println("Nothing deleted for key " + key);
        return Status.NOT_FOUND;
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    INIT_COUNT.incrementAndGet();
    synchronized (INCLUDE) {
      if (mongoClient != null) {
        return;
      }

      Properties props = getProperties();

      // Set insert batchsize, default 1 - to be YCSB-original equivalent
      batchSize = Integer.parseInt(props.getProperty("batchsize", "1"));

      // Set is inserts are done as upserts. Defaults to false.
      useUpsert = Boolean.parseBoolean(
          props.getProperty("mongodb.upsert", "false"));

      // Just use the standard connection format URL
      // http://docs.mongodb.org/manual/reference/connection-string/
      // to configure the client.
      String url = props.getProperty("mongodb.url", null);
      boolean defaultedUrl = false;
      if (url == null) {
        defaultedUrl = true;
        url = "mongodb://localhost:27017/ycsb?w=1";
      }

      url = OptionsSupport.updateUrl(url, props);

      if (!url.startsWith("mongodb://") && !url.startsWith("mongodb+srv://")) {
        System.err.println("ERROR: Invalid URL: '" + url
            + "'. Must be of the form "
            + "'mongodb://<host1>:<port1>,<host2>:<port2>/database?options' "
            + "or 'mongodb+srv://<host>/database?options'. "
            + "http://docs.mongodb.org/manual/reference/connection-string/");
        System.exit(1);
      }

      try {
        MongoClientURI uri = new MongoClientURI(url);

        String uriDb = uri.getDatabase();
        if (!defaultedUrl && (uriDb != null) && !uriDb.isEmpty()
            && !"admin".equals(uriDb)) {
          databaseName = uriDb;
        } else {
          // If no database is specified in URI, use "ycsb"
          databaseName = "ycsb";

        }

        readPreference = uri.getOptions().getReadPreference();
        writeConcern = uri.getOptions().getWriteConcern();

        mongoClient = new MongoClient(uri);
        database =
            mongoClient.getDatabase(databaseName)
                .withReadPreference(readPreference)
                .withWriteConcern(writeConcern);

        yelpDatabase = mongoClient.getDatabase(yelpDatabaseName);
        yelpCollection = yelpDatabase.getCollection(yelpCollectionName);
        iterator = yelpCollection.find().iterator();


        System.out.println("mongo client connection created with " + url);
      } catch (Exception e1) {
        System.err
            .println("Could not initialize MongoDB connection pool for Loader: "
                + e1.toString());
        e1.printStackTrace();
        return;
      }
    }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to insert.
   * @param values
   *          A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status insert(String table, String key,
      Map<String, ByteIterator> values) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      Document toInsert = (Document) iterator.next();
      System.out.println(toInsert.toString());
      toInsert.remove("_id");
      toInsert.append("_id", key);

      Document attributes = (Document) toInsert.get("attributes");
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        if(entry.getKey().equals("field9")) {
          attributes.append("Abstract", entry.getValue().toString());
        }
      }
      toInsert.put("attributes", attributes);




//      Document toInsert = new Document("_id", key);
//      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
//
//        if (entry.getKey().equals("field0")) {
//          toInsert.put("full_address", entry.getValue().toArray());
//        } else if (entry.getKey().equals("field1"))  {
//          Document hours = getHours();
//          toInsert.put("hours", hours);
//        } else if (entry.getKey().equals("field2"))  {
//          toInsert.append("city", "Phoenix");
//        } else if (entry.getKey().equals("field3")) {
//          toInsert.append("review_count", 4);
//        } else if (entry.getKey().equals("field4")) {
//          toInsert.put("longitude", Double.parseDouble(entry.getValue().toString()));
//        } else if (entry.getKey().equals("field5")) {
//          toInsert.put("latitude", Double.parseDouble(entry.getValue().toString()));
//        } else if (entry.getKey().equals("field7")) {
//          toInsert.put("categories", buildCategoryField());
//        } else if (entry.getKey().equals("field8")) {
//          toInsert.put("stars", buildRandomRate(2));
//        } else {
//          toInsert.put(entry.getKey(), entry.getValue().toArray());
//        }
//
//        Document attributes = getAttributes();
//        toInsert.put("attributes", attributes);
//
//      }

      if (batchSize == 1) {
        if (useUpsert) {
          // this is effectively an insert, but using an upsert instead due
          // to current inability of the framework to clean up after itself
          // between test runs.
          collection.replaceOne(new Document("_id", toInsert.get("_id")),
              toInsert, UPDATE_WITH_UPSERT);
        } else {
          collection.insertOne(toInsert);
        }
      } else {
        bulkInserts.add(toInsert);
        if (bulkInserts.size() == batchSize) {
          if (useUpsert) {
            List<UpdateOneModel<Document>> updates = 
                new ArrayList<UpdateOneModel<Document>>(bulkInserts.size());
            for (Document doc : bulkInserts) {
              updates.add(new UpdateOneModel<Document>(
                  new Document("_id", doc.get("_id")),
                  doc, UPDATE_WITH_UPSERT));
            }
            collection.bulkWrite(updates);
          } else {
            collection.insertMany(bulkInserts, INSERT_UNORDERED);
          }
          bulkInserts.clear();
        } else {
          return Status.BATCHED_OK;
        }
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println("Exception while trying bulk insert with "
          + bulkInserts.size());
      e.printStackTrace();
      return Status.ERROR;
    }

  }

  private static Document getAttributes() {
    Random randomBoolean = new Random();
    Document parking = new Document();
    parking.put("garage", randomBoolean.nextBoolean());
    parking.put("street", randomBoolean.nextBoolean());
    parking.put("validated", randomBoolean.nextBoolean());
    parking.put("lot", randomBoolean.nextBoolean());
    parking.put("valet", randomBoolean.nextBoolean());
    Document attributes = new Document();
    attributes.put("Parking", parking);
    attributes.put("Price range", 1);
    return attributes;
  }

  private String buildCategoryField() {

    String [] categories = new String[] {"Restaurant", "Bar", "Cafe", "Women's Clothing",
        "Fashion", "Shopping", "Home & Garten", "Furniture Stores", "Drugstores",
        "Food", "Convenience Stores", "Beauty & Spas", "Cosmetic & Beauty Supply", "Sports Wear"};
    int size = categories.length;
    int randomElement = (int)(Math.random() * size);
    return categories[randomElement];
  }

  private float buildRandomRate(int minRate) {
    int size = 31;
    float [] rates = new float[size];
    float tmp;
    for (int i = 0; i < size; i++) {
      tmp = (minRate * 10 + i);
      rates[i] = (float) Math.round(tmp) /10;
    }
    int randomElement = (int)(Math.random() * size);
    return rates[randomElement];
  }

  private static Document getHours() {
    Document hours = new Document();
    Document monday = new Document();
    monday.put("open", "18:00");
    monday.put("close", "20:00");
    Document tuesday = new Document();
    tuesday.put("open", "13:00");
    tuesday.put("close", "20:00");
    Document wednesday = new Document();
    wednesday.put("open", "8:00");
    wednesday.put("close", "15:00");
    Document thursday = new Document();
    thursday.put("open", "10:00");
    thursday.put("close", "22:00");
    Document friday = new Document();
    friday.put("open", "10:00");
    friday.put("close", "22:00");
    Document saturday = new Document();
    saturday.put("open", "10:00");
    saturday.put("close", "22:00");
    Document sunday = new Document();
    sunday.put("open", "10:00");
    sunday.put("close", "22:00");
    hours.put("monday", monday);
    hours.put("tuesday", tuesday);
    hours.put("wednesday", wednesday);
    hours.put("thursday", thursday);
    hours.put("friday", friday);
    hours.put("saturday", saturday);
    hours.put("sunday", sunday);
    return hours;
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to read.
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);
//      Document query = new Document("_id", key);
      Document query = new Document("attributes.Abstract" , key);


      //db.usertable.find({"categories" : "Sports Wear", "attributes.Parking.garage" : true})



      FindIterable<Document> findIterable = collection.find(query);

      if (fields != null) {
        Document projection = new Document();
        for (String field : fields) {
          projection.put(field, INCLUDE);
        }
        findIterable.projection(projection);
      }

      Document queryResult = findIterable.first();

      if (queryResult != null) {
        fillMap(result, queryResult);
      }
      return queryResult != null ? Status.OK : Status.NOT_FOUND;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   * 
   * @param table
   *          The name of the table
   * @param startkey
   *          The record key of the first record to read.
   * @param recordcount
   *          The number of records to read
   * @param fields
   *          The list of fields to read, or null for all of them
   * @param result
   *          A Vector of HashMaps, where each HashMap is a set field/value
   *          pairs for one record
   * @return Zero on success, a non-zero error code on error. See the {@link DB}
   *         class's description for a discussion of error codes.
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {

    MongoCursor<Document> cursor = null;

    try {
      MongoCollection<Document> collection = database.getCollection(table);

      Document scanRange = new Document("$gte", startkey);
      Document query = new Document("_id", scanRange);
      Document sort = new Document("_id", INCLUDE);


      FindIterable<Document> findIterable =
          collection.find(query).sort(sort).limit(recordcount);

      if (fields != null) {
        Document projection = new Document();
        for (String fieldName : fields) {
          projection.put(fieldName, INCLUDE);
        }
        findIterable.projection(projection);
      }

      cursor = findIterable.iterator();

      if (!cursor.hasNext()) {
        System.err.println("Nothing found in scan for key " + startkey);
        return Status.ERROR;
      }

      result.ensureCapacity(recordcount);

      while (cursor.hasNext()) {
        HashMap<String, ByteIterator> resultMap =
            new HashMap<String, ByteIterator>();

        Document obj = cursor.next();
        fillMap(resultMap, obj);

        result.add(resultMap);
      }

      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    } finally {
      if (cursor != null) {
        cursor.close();
      }
    }
  }

  @Override
  public  Status scanCoordinates(String table, double  startLongitude, double startLatitude, int recordcount,
                                         Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {

    MongoCursor<Document> cursor = null;

    double endLongitude = startLongitude + LONGITUDE_RANGE;
    double endLatitude = startLatitude + LATITUDE_RANGE;



    try {
      MongoCollection<Document> collection = database.getCollection(table);


//      Document scanRangeModified = new Document(and(lt("_id", endkey), gt("_id", startkey)));

//      Document scanRange = new Document("$gte", startkey);
//
//      Document scanRange2 = new Document("$lt", endkey);

//      Document query = new Document("_id", scanRange);
//      Document query2 = new Document("_id", scanRange2);

//      Document query = new Document("attributes.Parking.garage", true);
//      Document query2 = new Document("attributes.Parking.street", true);




      Document sort = new Document("_id", INCLUDE);

//      FindIterable <Document> findIterable = collection.find(Filters.lt("gte", startkey)).sort(sort)
//      .limit(recordcount);

      Bson longitudeQuery = Filters.and(Filters.gte("longitude", startLongitude),
          Filters.lt("longitude", endLongitude));
      Bson latitudeQuery = Filters.and(Filters.gte("latitude", startLatitude),
          Filters.lt("latitude", endLatitude));

//      FindIterable <Document> findIterable = collection.find(longitudeQuery).sort(sort).limit(recordcount);
//      FindIterable <Document> findIterable = collection.find(latitudeQuery).sort(sort).limit(recordcount);
      FindIterable <Document> findIterable = collection.find(
          Filters.and(longitudeQuery, latitudeQuery)).sort(sort).limit(recordcount);

//      FindIterable <Document> findIterable = collection.find(Filters.and(Filters.gte("longitude", startLongitude),
//          Filters.lt("longitude", endLongitude))).sort(sort).limit(recordcount);

//      FindIterable<Document> findIterable =
//          collection.find(Filters.and(query2, query)).sort(sort).limit(recordcount);

      if (fields != null) {
        Document projection = new Document();
        for (String fieldName : fields) {
          projection.put(fieldName, INCLUDE);
        }
        findIterable.projection(projection);
      }

      cursor = findIterable.iterator();

      if (!cursor.hasNext()) {
        System.err.println("Nothing found in scan for coordinates: (" + startLongitude + ", " + startLatitude +")");
        return Status.ERROR;
      }

      result.ensureCapacity(recordcount);

      while (cursor.hasNext()) {
        HashMap<String, ByteIterator> resultMap =
            new HashMap<String, ByteIterator>();

        Document obj = cursor.next();
        fillMap(resultMap, obj);

        result.add(resultMap);
      }

      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    } finally {
      if (cursor != null) {
        cursor.close();
      }
    }
  }


  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   * 
   * @param table
   *          The name of the table
   * @param key
   *          The record key of the record to write.
   * @param values
   *          A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error. See this class's
   *         description for a discussion of error codes.
   */
  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    try {
      MongoCollection<Document> collection = database.getCollection(table);

      Document query = new Document("_id", key);
      Document fieldsToSet = new Document();
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        fieldsToSet.put(entry.getKey(), entry.getValue().toArray());
      }
      Document update = new Document("$set", fieldsToSet);

      UpdateResult result = collection.updateOne(query, update);
      if (result.wasAcknowledged() && result.getMatchedCount() == 0) {
        System.err.println("Nothing updated for key " + key);
        return Status.NOT_FOUND;
      }
      return Status.OK;
    } catch (Exception e) {
      System.err.println(e.toString());
      return Status.ERROR;
    }
  }

  /**
   * Fills the map with the values from the DBObject.
   * 
   * @param resultMap
   *          The map to fill/
   * @param obj
   *          The object to copy values from.
   */
  protected void fillMap(Map<String, ByteIterator> resultMap, Document obj) {
    for (Map.Entry<String, Object> entry : obj.entrySet()) {
      if (entry.getValue() instanceof Binary) {
        resultMap.put(entry.getKey(),
            new ByteArrayByteIterator(((Binary) entry.getValue()).getData()));
      }
    }
  }
}
