package com.netflix.edda

import com.netflix.edda.aws.AwsClient
import com.netflix.edda.aws.AwsBeanMapper
import com.netflix.edda.basic.BasicBeanMapper
import com.netflix.edda.aws.AwsCollectionBuilder
import com.netflix.edda.basic.BasicContext

import org.slf4j.LoggerFactory

object EntryPoint {
	
  private[this] val logger = LoggerFactory.getLogger(getClass)

  
  def main(args: Array[String]): Unit = {
    
    // Check parameters
    if(args.length < 3) {
      println("Not enought parameters.")
      println("Usage: ./teevity-scala <region> <accessKey> <secretKey>")
      exit(-1)
    }
    
    val _region = args(0)
    val _accessKey = args(1)
    val _secretKey = args(2)
    
    // Initialize properties
    Utils.initConfiguration(System.getProperty("edda.properties","edda.properties"))

    logger.info(String.format("Starting fetching for AccessKey=[%s].", _accessKey))
    
    // use MongoDb as Datastore
    val datastoreClassName = Utils.getProperty("edda", "datastore.class", "", "com.netflix.edda.mongo.MongoDatastore").get
    val datastoreClass = this.getClass.getClassLoader.loadClass(datastoreClassName)
    val datastoreCtor = datastoreClass.getConstructor(classOf[String])

    val dsFactory = (name: String) => Some(datastoreCtor.newInstance(name).asInstanceOf[Datastore])

    // Use Mongo as Elector
    val electorClassName = Utils.getProperty("edda", "elector.class", "", "com.netflix.edda.mongo.MongoElector").get
    val electorClass = this.getClass.getClassLoader.loadClass(electorClassName)

    val elector = electorClass.newInstance.asInstanceOf[Elector]

    val bm = new BasicBeanMapper with AwsBeanMapper
    
    val awsClientFactory = (account: String) => {
    	new AwsClient(_accessKey, _secretKey, _region)
    }
    
    // Add factory into collections
    AwsCollectionBuilder.buildAll(BasicContext, awsClientFactory, bm, elector, dsFactory)

    logger.info("Starting Collections")
    // Crawl for all account
    CollectionManager.start()
    
    
  }
  
  
  
  
  
}