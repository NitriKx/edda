/**
 * Copyright 2012 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.edda.elasticsearch

import com.netflix.edda.Record
import com.netflix.edda.Collection
import com.netflix.edda.DataStore
import com.netflix.edda.Utils

// // http://www.mongodb.org/display/DOCS/Java+Tutorial

// import com.mongodb.BasicDBObject
// import com.mongodb.DBObject
// import com.mongodb.BasicDBList
// import com.mongodb.Mongo
// import com.mongodb.MongoOptions
// import com.mongodb.ServerAddress
// import com.mongodb.Bytes

import org.joda.time.DateTime
import java.util.Date
// import java.util.Properties

import org.slf4j.LoggerFactory

import org.elasticsearch.index.query.FilterBuilders
import org.elasticsearch.index.query.FilterBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.action.search.SearchRequestBuilder


// /** helper object to store common Mongo related routines */
object ElasticSearchDatastore {

//   val nullLtimeQuery = mapToMongo(Map("ltime" -> null))
//   val stimeIdSort = mapToMongo(Map("stime" -> -1, "id" -> 1))

  import org.joda.time.format.ISODateTimeFormat
  val basicDateTime = ISODateTimeFormat.dateTime

  /** converts a mongo DBObject to a Record */
  def esToRecord(obj: Any): Record = {
    obj match {
      case o: java.util.Map[_,_] =>
        Record(
          Option(o.get("id")).getOrElse(o.get("_id")).asInstanceOf[String],
          basicDateTime.parseDateTime(o.get("ctime").asInstanceOf[String]),
          basicDateTime.parseDateTime(Option(o.get("stime")).getOrElse(o.get("ctime")).asInstanceOf[String]),
          Option(o.get("ltime")) match {
            case Some(date: String) => basicDateTime.parseDateTime(date)
            case None => null
          },
          basicDateTime.parseDateTime(o.get("mtime").asInstanceOf[String]),
          esToScala(o.get("data")),
          esToScala(o.get("tags")).asInstanceOf[Map[String, Any]])
      case other => throw new java.lang.RuntimeException("cannot turn " + other + " into a Record")
    }
  }

  private val dateTimeRx = """^\d\d\d\d\d\d\d\dT\d\d\d\d\d\d([.]\d\d?\d?)?Z$""".r

  /** converts a mongo object to a corresponding Scala basic object */
  def esToScala(obj: Any): Any = {
    import collection.JavaConverters._
    obj match {
      case o: java.util.Map[_,_] => {
        o.keySet.asScala.map(key => (key.asInstanceOf[String] -> esToScala(o.get(key)))).toMap
      }
      case o: java.util.Collection[_] => {
        List.empty[Any] ++ o.asScala.map(esToScala(_))
      }
      case dateTimeRx() => basicDateTime.parseDateTime(obj.asInstanceOf[String])
      case o: Date => new DateTime(o)
      case o: AnyRef => o
      case null => null
      case other => throw new java.lang.RuntimeException("mongoToScala: don't know how to handle: " + other)
    }
  }

//   /** converts a Record to a Mongo DBObject */
//   def recordToMongo(rec: Record, id: Option[String] = None): DBObject = {
//     val obj = mapToMongo(rec.toMap)
//     if (id.isDefined) {
//       obj.put("_id", id.get)
//     } else {
//       obj.put("_id", rec.id + "|" + rec.stime.getMillis)
//     }
//     obj
//   }

//   /** converts a basic scala Map to a Mongo DBObject */
//   def mapToMongo(map: Map[String, Any]): DBObject = {
//     val obj = new BasicDBObject
//     map.foreach(pair => obj.put(pair._1, scalaToMongo(pair._2)))
//     obj
//   }

//   /** converts a Scala basic type to a corresponding Mongo data type */
//   def scalaToMongo(obj: Any): AnyRef = {
//     obj match {
//       case o: Map[_, _] => mapToMongo(o.asInstanceOf[Map[String, Any]])
//       case o: Seq[_] => {
//         val mongo = new BasicDBList
//         o.foreach(item => mongo.add(scalaToMongo(item)))
//         mongo
//       }
//       case o: DateTime => o.toDate
//       case o: AnyRef => o
//       case null => null
//       case other => throw new java.lang.RuntimeException("scalaToMongo: don't know how to handle: " + other)
//     }
//   }

//   /** helper to fetch properties for mongo related settings */
//   def mongoProperty(propName: String, dsName: String, dflt: String): String = {
//     Utils.getProperty("edda", "mongo." + propName, "datastore." + dsName, dflt).get
//   }

//   var primaryMongoConnections: Map[String,Mongo] = Map()
//   var replicaMongoConnections: Map[String,Mongo] = Map()

//   /** from the collection name string return a Mongo DB Connection */
//   def mongoConnection(name: String, replicaOk: Boolean = false): Mongo = {
//     import collection.JavaConverters._
//     val servers = mongoProperty("address", name, "");
//     if( replicaOk && replicaMongoConnections.contains(servers) ) 
//         replicaMongoConnections(servers)
//     else if( !replicaOk && primaryMongoConnections.contains(servers) ) 
//         primaryMongoConnections(servers)
//     else {
//         val serverList = util.Random.shuffle(
//             servers.split(',').map(
//                 hostport => {
//                     val parts = hostport.split(':')
//                     if (parts.length > 1) {
//                         new ServerAddress(parts(0), parts(1).toInt)
//                     } else {
//                         new ServerAddress(parts(0))
//                     }
//                 }).toList
//         )

//         val queryTimeout = Utils.getProperty("edda.collection", "queryTimeout", name, "60000").get.toInt

//         val options = new MongoOptions
//         options.autoConnectRetry = true
//         options.connectTimeout = 500
//         options.connectionsPerHost = 40
//         options.socketKeepAlive = true
//         options.socketTimeout = queryTimeout
//         options.threadsAllowedToBlockForConnectionMultiplier = 8
        
//         val primary = new Mongo(serverList.asJava, options)
//         primaryMongoConnections += (servers -> primary)

//         val replica = new Mongo(serverList.asJava, options)
//         replica.slaveOk()
//         replicaMongoConnections += (servers -> replica)

//         if(replicaOk) replica else primary
//     }
//   }

//   /** from the collection name string return a Mongo Collection (creates the collection
//     * if it does not exist) */
//   def mongoCollection(name: String, replicaOk: Boolean = false) = {
//     val conn = mongoConnection(name, replicaOk)
//     val db = conn.getDB(mongoProperty("database", name, "edda"))
//     val user = mongoProperty("user", name, null)
//     if (user != null) {
//       db.authenticate(
//         user,
//         mongoProperty("password", name, "").toArray)
//     }
//     if (db.collectionExists(name)) db.getCollection(name) else db.createCollection(name, null)
//   }

  /** dispatch the match operator to the correct matching routine. */
  protected def esFilterOp(key: String, value: Any, op: String): FilterBuilder = {
    // $eq $ne $gt $lt $gte $lte $exists $in $nin $regex
    op match {
      case "$eq" => Option(value) match {
        case None => FilterBuilders.missingFilter(key).nullValue(true).existence(true)
        case Some(value) => FilterBuilders.termFilter(key, value)
      }
      case "$ne" => FilterBuilders.notFilter(esFilterOp(key, value, "$eq"))
      case "$gt" => value match {
        case v: String => FilterBuilders.rangeFilter(key).from(v).includeLower(false)
        case _ => FilterBuilders.numericRangeFilter(key).from(value).includeLower(false)
      }
      case "$gte" => value match {
        case v: String => FilterBuilders.rangeFilter(key).from(v).includeLower(true)
        case _ => FilterBuilders.numericRangeFilter(key).from(value).includeLower(true)
      }
      case "$lt" => value match {
        case v: String => FilterBuilders.rangeFilter(key).to(v).includeUpper(false)
        case _ => FilterBuilders.numericRangeFilter(key).to(value).includeUpper(false)
      }
      case "$lte" => value match {
        case v: String => FilterBuilders.rangeFilter(key).to(v).includeUpper(true)
        case _ => FilterBuilders.numericRangeFilter(key).to(value).includeUpper(true)
      }
      case "$exists" => FilterBuilders.missingFilter(key).existence(true)
      case "$in" => value.asInstanceOf[Seq[Any]].head match {
        case _: String => FilterBuilders.inFilter(key, value.asInstanceOf[Seq[String]].toArray)
        case _: Long => FilterBuilders.inFilter(key, value.asInstanceOf[Seq[Long]].toArray)
        case _: Int => FilterBuilders.inFilter(key, value.asInstanceOf[Seq[Int]].toArray)
        case _: Double => FilterBuilders.inFilter(key, value.asInstanceOf[Seq[Double]].toArray)
        case _: Float => FilterBuilders.inFilter(key, value.asInstanceOf[Seq[Float]].toArray)
        case _: AnyRef => FilterBuilders.inFilter(key, value.asInstanceOf[Seq[AnyRef]].toArray)
      }
      case "$nin" => FilterBuilders.notFilter(esFilterOp(key, value, "$in"))
      case "$regex" => throw new java.lang.UnsupportedOperationException("$regex query not supported")
      case unk => throw new java.lang.RuntimeException("uknown match operation: " + unk)
    }
  }

  def esFilter(queryMap: Map[String, Any]): FilterBuilder = {
    val filters = queryMap.map {
      // { key: { $op1: val, $op2: val } } ==>
      case (key: String, value: Map[_, _]) => {
        if( value.size > 1 ) {
          val andFilter = FilterBuilders.andFilter()
          value.asInstanceOf[Map[String,Any]].foreach( kv => andFilter.add(esFilter( Map(key -> (kv._1, kv._2)) )) )
          andFilter
        } else {
          val kv = value.asInstanceOf[Map[String,Any]].head
          esFilter( Map(key -> (kv._1, kv._2)) )
        }
      }
      // { $or: [ {key: value}, {key: value} ] }
      case ("$or", value: Seq[_]) => {
        val filters = value.asInstanceOf[Seq[Map[String,Any]]].map(esFilter(_)).toArray
        val orFilter = FilterBuilders.orFilter()
        filters.foreach( orFilter.add(_) )
        orFilter
      }
      // { $and: [ {key: value}, {key: value} ] }
      case ("$and", value: Seq[_]) => {
        val filters = value.asInstanceOf[Seq[Map[String,Any]]].map(esFilter(_)).toArray
        val andFilter = FilterBuilders.andFilter()
        filters.foreach( andFilter.add(_) )
        andFilter
      }
      // { key1: { $op1: val }, key2: { $op2: val } }
      case (key: String, (op: String, value: Any)) => esFilterOp(key, value, op)
      case (key: String, value: Any) => esFilterOp(key,value,"$eq")
      case (key: String, null) => esFilterOp(key,null,"$eq")
    } toSeq
    
    if( queryMap.size > 1 ) {
      val andFilter = FilterBuilders.andFilter()
      filters.foreach( andFilter.add(_) )
      andFilter
    } else filters.head
  }

  def esQuery(queryMap: Map[String, Any]): QueryBuilder = {
    if( queryMap.isEmpty ) QueryBuilders.matchAllQuery else QueryBuilders.constantScoreQuery(esFilter(queryMap))
  }
}

/** [[com.netflix.edda.DataStore]] subclass that allows MongoDB to be used
 *
 * @param name the name of the collection the datastore is for
 */
class ElasticSearchDatastore(val name: String) extends DataStore {

  import Collection.RetentionPolicy._
  import ElasticSearchDatastore._

  import org.elasticsearch.common.settings.ImmutableSettings
  import org.elasticsearch.common.settings.Settings
  import org.elasticsearch.client.transport.TransportClient
  import org.elasticsearch.client.Client
  import org.elasticsearch.common.transport.InetSocketTransportAddress
  import org.elasticsearch.action.search.SearchOperationThreading._

  lazy val settings: Settings = ImmutableSettings.settingsBuilder().put("cluster.name", Utils.getProperty("edda", "elasticsearch.cluster", name, "edda").get).build()
  lazy val client: Client = Utils.getProperty("edda", "elasticsearch.address", name, "edda").get.split(',').fold(new TransportClient(settings))(
    (client, addr) => {
      val parts = addr.asInstanceOf[String].split(':')
      client.asInstanceOf[TransportClient].addTransportAddress(new InetSocketTransportAddress(parts.head, parts.tail.head.toInt))
    }
  ).asInstanceOf[Client]
        
  private[this] val logger = LoggerFactory.getLogger(getClass)

  def init() {
    // TODO create index if missing (set replication and shards), add/update mapping
  }

  /** perform query on data store, see [[com.netflix.edda.Queryable.query()]] */
  def query(queryMap: Map[String, Any], limit: Int, keys: Set[String], replicaOk: Boolean): Seq[Record] = {
    var builder = client.prepareSearch(name.toLowerCase).setQuery(esQuery(queryMap));
    if( !replicaOk ) builder = builder.setPreference("_primary")
    if( limit > 0 ) fetch(builder, limit) else scan(builder)
  }

  /** load records from data store, used at Collection start-up to prime in-memory cache and to refresh
    * in-memory cache when we are not the leader
    *
    * @param replicaOk specify if we can load from a read-replica in the data store when there are
    *                  redundant systems running for high-availability.
    */
  def load(replicaOk: Boolean): Seq[Record] = {
    var builder = client.prepareSearch(name.toLowerCase + ".live");
    if( !replicaOk ) builder = builder.setPreference("_primary")
    scan(builder)
  }

  def fetch(search: SearchRequestBuilder, limit: Int): Seq[Record] = {
    import collection.JavaConverters.iterableAsScalaIterableConverter
    import org.elasticsearch.action.search.SearchResponse
    import org.elasticsearch.action.search.SearchType
    val searchResp = search.setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setFrom(0).setSize(limit).execute().actionGet();
    searchResp.getHits().asScala.map(r => {
      try esToRecord(r.getSource)    
      catch {
        case e: Exception => {
          logger.error(this + " failed to parse record: " + r.getSource, e)
          throw e
        }
      }
    }) toSeq
  }

  def scan( search: SearchRequestBuilder): Seq[Record] = {
    import collection.JavaConverters.iterableAsScalaIterableConverter
    import org.elasticsearch.action.search.SearchResponse
    import org.elasticsearch.action.search.SearchType
    import org.elasticsearch.common.unit.TimeValue
    var scrollResp: SearchResponse = search.
      setSearchType(SearchType.SCAN).
      setScroll(new TimeValue(60000)).
      setSize(100).execute().actionGet(); //100 hits per shard will be returned for each scroll
    
    //Scroll until no hits are returned
    var keepLooping = true
    var seq: Seq[Record] = Seq()

    while (keepLooping) {
      scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet()
      seq = seq ++ scrollResp.getHits().asScala.map(r => {
        try esToRecord(r.getSource)
        catch {
          case e: Exception => {
            logger.error(this + " failed to parse record: " + r.getSource, e)
            throw e
          }
        }
      })
      
      //Break condition: No hits are returned
      if (scrollResp.hits().hits().length == 0) {
        keepLooping = false
      }
    }
    seq
  }

  
  /** make changes to the data store depending on the Collection delta found after a Crawl result */
  def update(d: Collection.Delta) {}

  /** remove records that match the query */
  def remove(queryMap: Map[String, Any]) {}

  /** when was the last time the collection was updated */
  def collectionModified: DateTime = DateTime.now

  // lazy val primary = mongoCollection(name)
  // lazy val replica = mongoCollection(name, replicaOk=true)
  // lazy val monitor = mongoCollection(Utils.getProperty("edda.mongo", "monitor.collectionName", "name", "sys.monitor").get)

  // lazy val retentionPolicy = Utils.getProperty("edda.collection", "retentionPolicy", name, "ALL")

  // private[this] val logger = LoggerFactory.getLogger(getClass)

  // /** query routine to fetch records from mongoDB.
  //   *
  //   * @param queryMap query criteria
  //   * @param limit restrict returned record count, 0 == unlimited
  //   * @param keys  unless empty Set return only requested keys
  //   * @param replicaOk reading from a replica in a replSet is OK this is set to true
  //   * @return the records that match the query
  //   */
  // override def query(queryMap: Map[String, Any], limit: Int, keys: Set[String], replicaOk: Boolean): Seq[Record] = {
  //   import collection.JavaConverters.iterableAsScalaIterableConverter
  //   val mtime = collectionModified
  //   val mongoKeys = if (keys.isEmpty) null else mapToMongo(keys.map(_ -> 1).toMap)
  //   val t0 = System.nanoTime()
  //   val cursor = {
  //     val mongo = if(replicaOk) replica else primary
  //     val cur = mongo.find(mapToMongo(queryMap), mongoKeys)
  //     if( limit > 0 ) cur.sort(stimeIdSort).limit(limit) else cur.sort(stimeIdSort)
  //   }
  //   try {
  //     cursor.asScala.toStream.map(mongoToRecord(_)).map(r => if(r.ltime == null ) r.copy(mtime=mtime) else r)
  //   } catch {
  //      case e: Exception => {
  //           logger.error(this + " query failed: " + queryMap + " limit: " + limit + " keys: " + keys + " replicaOk: " + replicaOk, e)
  //           throw e
  //       }
  //   } finally {
  //     val t1 = System.nanoTime()
  //     val lapse = (t1 - t0) / 1000000;
  //     logger.info(this + " query: " + queryMap + " lapse: " + lapse + "ms")
  //     cursor.close()
  //   }
  // }

  // /** load records from the collection MongoDB table
  //   *
  //   * @param replicaOk reading from a replica in a replSet is OK if this is set to true
  //   * @return the active records (ltime == null) from the collection
  //   */
  // override def load(replicaOk: Boolean): Seq[Record] = {
  //   import collection.JavaConverters.iterableAsScalaIterableConverter
  //   val mtime = collectionModified
  //   val cursor = {
  //     val mongo = if(replicaOk) replica else primary
  //     val cur = mongo.find(nullLtimeQuery)
  //     cur.sort(stimeIdSort)
  //   }
  //   try {
  //     val x = cursor.asScala.map(mongoToRecord(_)).toSeq.map(_.copy(mtime=mtime))
  //     logger.info(this + " Loaded " + x.size + " records")
  //     x
  //   } catch {
  //     case e: Exception => {
  //       throw new java.lang.RuntimeException(this + " failed to load", e)
  //     }
  //   } finally {
  //     cursor.close()
  //   }
  // }

  // /** update records, delete removed records, insert added records */
  // override def update(d: Collection.Delta) {
  //   var toRemove: Seq[Record] = Seq();
  //   val records = d.removed ++ d.added ++ d.changed.flatMap(
  //     pair => {
  //       // only update oldRecord if the stime is changed, this allows
  //       // for inplace updates when we dont want to create new document
  //       // revision, but still want the record updated
  //       if (pair.oldRecord.stime == pair.newRecord.stime) {
  //         Seq(pair.newRecord)
  //       } else if (Collection.RetentionPolicy.withName(retentionPolicy.get) == LAST) {
  //         toRemove = pair.oldRecord +: toRemove
  //         Seq(pair.newRecord)
  //       } else {
  //         Seq(pair.oldRecord, pair.newRecord)
  //       }
  //     })
    
  //   records.foreach( r => if (Collection.RetentionPolicy.withName(retentionPolicy.get) == LIVE && r.ltime != null) remove(r) else upsert(r) )
  //   toRemove.foreach( remove(_) )
  //   markCollectionModified
  // }

  // def collectionModified: DateTime  = {
  //     val rec = monitor.findOne(mapToMongo(Map("_id" -> name)));
  //     if( rec == null ) DateTime.now() else mongoToRecord(rec).mtime
  // }

  // def markCollectionModified = {
  //   try {
  //     val now = DateTime.now()
  //     monitor.findAndModify(
  //       mapToMongo(Map("_id" -> name)),
  //       null, // fields
  //       null, // sort
  //       false, // remove
  //       mapToMongo( // update
  //         Map(
  //         "_id" -> name,
  //         "id" -> name,
  //         "ctime" -> now,
  //         "mtime" -> now,
  //         "stime" -> now,
  //         "ltime" -> null,
  //         "data" -> Map("updated" -> now, "id" -> name, "type" -> "collection"))
  //       ),
  //       false, // returnNew
  //       true // upsert
  //     )
  //   } catch {
  //     case e: Exception => {
  //       logger.error(this + "failed to update collection mtime", e)
  //       throw e
  //     }
  //   }
  // }

  // /** ensures Indes for "stime", "mtime", "ltime", and "id" */
  // def init() {
  //   primary.ensureIndex(mapToMongo(Map("stime" -> -1, "id" -> 1)))
  //   primary.ensureIndex(mapToMongo(Map("stime" -> -1)))
  //   primary.ensureIndex(mapToMongo(Map("mtime" -> -1)))
  //   primary.ensureIndex(mapToMongo(Map("ltime" -> 1)))
  //   primary.ensureIndex(mapToMongo(Map("id" -> 1)))
  // }

  // protected def upsert(record: Record) {
  //   try {
  //     primary.findAndModify(
  //       mapToMongo(Map("_id" -> (record.id + "|" + record.stime.getMillis))), // query
  //       null, // fields
  //       null, // sort
  //       false, // remove
  //       recordToMongo(record), // update
  //       false, // returnNew
  //       true // upsert
  //     )
  //   } catch {
  //     case e: Exception => {
  //       logger.error("failed to upsert record: " + record)
  //       throw e
  //     }
  //   }
  // }

  // protected def remove(record: Record) {
  //   remove(Map("_id" -> (record.id + "|" + record.stime.getMillis)));
  // }

  // override def remove(queryMap: Map[String, Any]) {
  //   try {
  //     primary.remove(mapToMongo(queryMap))
  //   } catch {
  //     case e: Exception => {
  //       logger.error("failed to remove records: " + queryMap)
  //       throw e
  //     }
  //   }
  // }
    
  override def toString = "[ElasticSearchDatastore " + name + "]"
}
