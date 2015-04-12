/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package cz.ctu.fit.lhd.loader;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import java.net.UnknownHostException;

/**
 * @author Milan Dojchinovski
 * <milan (at) dojchinovski (dot) mk>
 * Twitter: @m1ci
 * www: http://dojchinovski.mk
 */
public class MongoDBClient {
        
    private static MongoClient   mongoClient = null;
    private static DB            db            = null;
    private static MongoDBClient mongodbclient = null;
    private BasicDBObject queryObj;
    private DBObject resObj = null;
            
    public static DB getDBInstance(String dbName){
        if(db == null){
            init();
            db = mongoClient.getDB( dbName );
            mongodbclient = new MongoDBClient();
        }
        return db;
    }
    
    public static MongoDBClient getInstance(String dbName){
        if(mongodbclient == null) {
            init();
            mongodbclient = new MongoDBClient();
            db = mongoClient.getDB( dbName );
        }
        return mongodbclient;
    }

    public static void init(){
        try {
            mongoClient = new MongoClient( "localhost" , 27017 );
        } catch (UnknownHostException ex) {
            //Logger.getLogger(MongoDBClient.class.getName()).log(Level.SEVERE, null, ex);
            //System.out.println(ex);           
        }
    }
}