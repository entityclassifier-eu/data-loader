/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package cz.ctu.fit.lhd.loader;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.SimpleSelector;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.util.FileManager;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * @author Milan Dojchinovski
 * <milan (at) dojchinovski (dot) mk>
 * Twitter: @m1ci
 * www: http://dojchinovski.mk
 */
public class RDFLoader {
        
    private String enDbpediaInstances;
    private String deDbpediaInstances;
    private String nlDbpediaInstances;
    
    private String yagoLabels;
    private String yagoTypes;    
    private String yagoMultilanguageLabels;
    
    private String enLHD10;
    private String deLHD10;
    private String nlLHD10;
    
    private String interlangLinksEN;
    private String interlangLinksDE;
    private String interlangLinksNL;
    private String dbName;
            
    private FileInputStream in = null;
    private BufferedReader  br = null;
    
    public void loadDataIntoDB(String settingsLocation, String datasets, String dbName){
        
        System.out.println( "Started loading data ..." );
        
        try {
            System.out.println("Settings location: " + settingsLocation);
            System.out.println("Datasets to load: " + datasets);
            System.out.println("Database name: " + dbName);
            this.dbName = dbName;

            Properties settings = new Properties();
            settings.load(new FileInputStream(settingsLocation));
            if (settings == null){
                System.out.println("Settings file not found!");
            }
            
            loadSettings(settings);
            if( datasets.equals("all") ){

                    loadDbpediaInstanceTypesDE();
                    loadDbpediaInstanceTypesEN();
                    loadDbpediaInstanceTypesNL();
                    
                    loadInterlanguageLinksEN();
                    loadInterlanguageLinksDE();
                    loadInterlanguageLinksNL();
                    
                    loadYAGOENLabels();
                    loadYAGOTypes();
                    loadYAGOMultilingualLabels();

                    loadLHD10EN();
                    loadLHD10DE();
                    loadLHD10NL();

            } else if(datasets.equals("loadDbpediaInstanceTypesEN")){
                    loadDbpediaInstanceTypesEN();
            
            } else if(datasets.equals("loadDbpediaInstanceTypesDE")){
                    loadDbpediaInstanceTypesDE();
            
            } else if(datasets.equals("loadDbpediaInstanceTypesNL")){
                    loadDbpediaInstanceTypesNL();
            
            } else if(datasets.equals("loadInterlanguageLinksEN")){
                    loadInterlanguageLinksEN();
            
            } else if(datasets.equals("loadInterlanguageLinksDE")){
                    loadInterlanguageLinksDE();
            
            } else if(datasets.equals("loadInterlanguageLinksNL")){
                    loadInterlanguageLinksNL();
            
            } else if(datasets.equals("loadYAGOENLabels")){
                    loadYAGOENLabels();
            
            } else if(datasets.equals("loadYAGOTypes")){
                    loadYAGOTypes();

            } else if(datasets.equals("loadYAGOMultilingualLabels")){
                    loadYAGOMultilingualLabels();
            
            } else if(datasets.equals("loadLHD10EN")){
                    loadLHD10EN();
            
            } else if(datasets.equals("loadLHD10DE")){
                    loadLHD10DE();
            
            } else if(datasets.equals("loadLHD10NL")){
                    loadLHD10NL();
                    
            } else {
                System.out.println("Required file for import was not found!");
                
            }
                    
        } catch (IOException ex) {
            Logger.getLogger(RDFLoader.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private void loadSettings(Properties settings) {
        
        enDbpediaInstances = settings.getProperty("enDbpediaInstances");
        deDbpediaInstances = settings.getProperty("deDbpediaInstances");
        nlDbpediaInstances = settings.getProperty("nlDbpediaInstances");
        
        yagoLabels = settings.getProperty("yagoLabels");
        yagoTypes = settings.getProperty("yagoTypes");
        yagoMultilanguageLabels = settings.getProperty("yagoMultilanguageLabels");
        
        enLHD10 = settings.getProperty("enLHD10");
        deLHD10 = settings.getProperty("deLHD10");
        nlLHD10 = settings.getProperty("nlLHD10");

        interlangLinksEN = settings.getProperty("interlangLinksEN");
        interlangLinksDE = settings.getProperty("interlangLinksDE");
        interlangLinksNL = settings.getProperty("interlangLinksNL");
        
        System.out.println("Settings loaded...");
    }

    BasicDBObject entry;
    RDFNode subject;
    RDFNode object;
    Statement stm;
       
    private void loadLHD10EN() {
        
        try {
            Model model = ModelFactory.createDefaultModel();
            InputStream in = FileManager.get().open( enLHD10);
            model.read(in, null, "N-TRIPLE");

            StmtIterator iter = model.listStatements( new SimpleSelector(null, null,  (RDFNode)null));

            while (iter.hasNext()) {
                Statement stm = iter.next();
                RDFNode subject = stm.getSubject();
                RDFNode object = stm.getObject();
                    
                    String[] sub = subject.toString().split("/");
                    String[] obj = object.toString().split("/");
                    String title = sub[sub.length-1].replace("_", " ");
                    String hypernym = obj[obj.length-1].replace("_", " ");
                    
                    BasicDBObject type = new BasicDBObject()
                                    .append("label", hypernym)
                                    .append("uri", object.toString())
                                    .append("origin", "thd")
                                    .append("accuracy", 0.857)
                                    .append("bounds", "+- 2.5%");
                    
                    if(object.toString().contains("/ontology/")) {
                        type.append("mapping","dbOnto");                    
                    } else if(object.toString().contains("/resource/")){
                        type.append("mapping","dbRes");
                    } else {
                        System.out.println("PROBLEM: "+ object.toString());
                    }
                    
                    BasicDBList types = new BasicDBList();
                    types.add(type);
                    
                    BasicDBObject entity = new BasicDBObject().append("types", types);
                    entity.append("label", title);
                    entity.append("uri", subject.toString());
                    
                    MongoDBClient.getDBInstance(dbName).getCollection("en_entities_thd_lhd10").insert(entity);
                        
           }

            System.out.println("Successfully imported entities from file: " + enLHD10);
        } catch (Exception ex) {
            Logger.getLogger(RDFLoader.class.getName()).log(Level.SEVERE, null, ex);
        }        
    }    
    
    private void loadLHD10DE() {
        try {
            Model model = ModelFactory.createDefaultModel();
            InputStream in = FileManager.get().open( deLHD10 );
            model.read(in, null, "N-TRIPLE");

            StmtIterator iter = model.listStatements( new SimpleSelector(null, null,  (RDFNode)null));

            while (iter.hasNext()) {
                Statement stm = iter.next();
                RDFNode subject = stm.getSubject();
                RDFNode object = stm.getObject();
                    
                    String[] sub = subject.toString().split("/");
                    String[] obj = object.toString().split("/");
                    String title = sub[sub.length-1].replace("_", " ");
                    ////System.out.print(title+" - ");
                    String hypernym = obj[obj.length-1].replace("_", " ");
                    ////System.out.println(hypernym);
                    
                    BasicDBObject type = new BasicDBObject()
                                    .append("label", hypernym)
                                    .append("uri", object.toString())
                                    .append("origin", "thd")
                                    .append("accuracy", 0.773)
                                    .append("bounds", "+- 2.5%");
                    
                    if(object.toString().contains("/ontology/")) {
                        type.append("mapping","dbOnto");                    
                    } else if(object.toString().contains("/resource/")){
                        type.append("mapping","dbRes");
                    } else {
                        System.out.println("PROBLEM: "+ object.toString());
                    }                        
                    
                    BasicDBList types = new BasicDBList();
                    types.add(type);
                    
                    BasicDBObject entity = new BasicDBObject().append("types", types);
                    entity.append("label", title);
                    entity.append("uri", subject.toString());
                    
                    MongoDBClient.getDBInstance(dbName).getCollection("de_entities_thd_lhd10").insert(entity);
                        
           }

            System.out.println("Successfully imported entities from file: " + deLHD10);
        } catch (Exception ex) {
            Logger.getLogger(RDFLoader.class.getName()).log(Level.SEVERE, null, ex);
        }        
    }    
        
    private void loadLHD10NL() {
        try {
            Model model = ModelFactory.createDefaultModel();
            InputStream in = FileManager.get().open( nlLHD10);
            model.read(in, null, "N-TRIPLE");

            StmtIterator iter = model.listStatements( new SimpleSelector(null, null,  (RDFNode)null));

            while (iter.hasNext()) {
                Statement stm = iter.next();
                RDFNode subject = stm.getSubject();
                RDFNode object = stm.getObject();
                    
                    String[] sub = subject.toString().split("/");
                    String[] obj = object.toString().split("/");
                    String title = sub[sub.length-1].replace("_", " ");
                    ////System.out.print(title+" - ");
                    String hypernym = obj[obj.length-1].replace("_", " ");
                    ////System.out.println(hypernym);
                    
                    BasicDBObject type = new BasicDBObject()
                                    .append("label", hypernym)
                                    .append("uri", object.toString())
                                    .append("origin", "thd")
                                    .append("accuracy", 0.884)
                                    .append("bounds", "+- 2.5%");
                    if(object.toString().contains("/ontology/")) {
                        type.append("mapping","dbOnto");                    
                    } else if(object.toString().contains("/resource/")){
                        type.append("mapping","dbRes");
                    } else {
                        System.out.println("PROBLEM: "+ object.toString());
                    }
                    
                    BasicDBList types = new BasicDBList();
                    types.add(type);
                    
                    BasicDBObject entity = new BasicDBObject().append("types", types);
                    entity.append("label", title);
                    entity.append("uri", subject.toString());
                    
                    MongoDBClient.getDBInstance(dbName).getCollection("nl_entities_thd_lhd10").insert(entity);
                        
           }

            System.out.println("Successfully imported entities from file: " + nlLHD10);
        } catch (Exception ex) {
            Logger.getLogger(RDFLoader.class.getName()).log(Level.SEVERE, null, ex);
        }        
    }    

    private void loadInterlanguageLinksEN() {
//                        
//                BasicDBObject entry = new BasicDBObject().append("$set", new BasicDBObject().append("de_uri", "dutch uri"));
//                MongoDBClient.getDBInstance().getCollection("test").update(
//                        new BasicDBObject().append("nl_uri", "dutch uri"), entry, true, false);

        Model model = ModelFactory.createDefaultModel();
        InputStream in = FileManager.get().open( interlangLinksEN);
        model.read(in, null, "TURTLE");
        System.out.println("Finished loading EN inter-language links dataset.");        
        
        StmtIterator iter = model.listStatements( new SimpleSelector(null, null,  (RDFNode)null));
        
        while (iter.hasNext()) {
            
            Statement stm = iter.next();
            RDFNode subject = stm.getSubject();
            RDFNode object = stm.getObject();

            if(object.toString().startsWith("http://de.dbpedia.org")) {
                
                DBObject clause1 = new BasicDBObject("en_uri", subject.toString());
                DBObject clause2 = new BasicDBObject("de_uri", object.toString());    
                BasicDBList or = new BasicDBList();
                or.add(clause1);
                or.add(clause2);

                entry = new BasicDBObject().append("$set", new BasicDBObject().append("en_uri", subject.toString()).append("de_uri", object.toString()));
                MongoDBClient.getDBInstance(dbName).getCollection("interlanguage_links").update(
                        new BasicDBObject("$or", or), entry, true, false);
                
            } else if(object.toString().startsWith("http://nl.dbpedia.org")) {
                
                DBObject clause1 = new BasicDBObject("en_uri", subject.toString());
                DBObject clause2 = new BasicDBObject("nl_uri", object.toString());    
                BasicDBList or = new BasicDBList();
                or.add(clause1);
                or.add(clause2);

                entry = new BasicDBObject().append("$set", new BasicDBObject().append("en_uri", subject.toString()).append("nl_uri", object.toString()));
                MongoDBClient.getDBInstance(dbName).getCollection("interlanguage_links").update(
                        new BasicDBObject("$or", or), entry, true, false);
            }
        }
        System.out.println("Finished inserting EN inter-language links.");        
    }
    
    private void loadInterlanguageLinksDE() {
//                DBObject clause1 = new BasicDBObject("de_uri", "de uri");
//                DBObject clause2 = new BasicDBObject("en_uri", "en uri");
//                BasicDBList or = new BasicDBList();
//                or.add(clause1);
//                or.add(clause2);
//
//                entry = new BasicDBObject().append("$set", new BasicDBObject().append("en_uri", "en uri").append("de_uri", "de uri"));
//                MongoDBClient.getDBInstance().getCollection("test").update(
//                        new BasicDBObject("$or", or), entry, true, false);
//        
        Model model = ModelFactory.createDefaultModel();
        InputStream in = FileManager.get().open( interlangLinksDE );
        model.read(in, null, "TURTLE");
               
        System.out.println("Finished loading DE inter-language links dataset.");        
        
        StmtIterator iter = model.listStatements( new SimpleSelector(null, null,  (RDFNode)null));
        
        while (iter.hasNext()) {
            
            Statement stm = iter.next();
            RDFNode subject = stm.getSubject();
            RDFNode object = stm.getObject();

            if(object.toString().startsWith("http://dbpedia.org")) {
                
                DBObject clause1 = new BasicDBObject("de_uri", subject.toString());
                DBObject clause2 = new BasicDBObject("en_uri", object.toString());    
                BasicDBList or = new BasicDBList();
                or.add(clause1);
                or.add(clause2);

                entry = new BasicDBObject().append("$set", new BasicDBObject().append("de_uri", subject.toString()).append("en_uri", object.toString()));
                MongoDBClient.getDBInstance(dbName).getCollection("interlanguage_links").update(
                        new BasicDBObject("$or", or), entry, true, false);
                
            } else if(object.toString().startsWith("http://nl.dbpedia.org")) {
                
                DBObject clause1 = new BasicDBObject("de_uri", subject.toString());
                DBObject clause2 = new BasicDBObject("nl_uri", object.toString());    
                BasicDBList or = new BasicDBList();
                or.add(clause1);
                or.add(clause2);

                entry = new BasicDBObject().append("$set", new BasicDBObject().append("de_uri", subject.toString()).append("nl_uri", object.toString()));
                MongoDBClient.getDBInstance(dbName).getCollection("interlanguage_links").update(
                        new BasicDBObject("$or", or), entry, true, false);
            }            
        }
        System.out.println("Finished inserting DE inter-language links.");        
    }

    private void loadInterlanguageLinksNL() {
                
        Model model = ModelFactory.createDefaultModel();
        InputStream in = FileManager.get().open( interlangLinksNL );
        model.read(in, null, "TURTLE");
       
        System.out.println("Finished loading NL inter-language links dataset.");        
        
        StmtIterator iter = model.listStatements( new SimpleSelector(null, null,  (RDFNode)null));
        
        while (iter.hasNext()) {
            
            stm = iter.next();
            subject = stm.getSubject();
            object = stm.getObject();

            if(object.toString().startsWith("http://dbpedia.org")) {
                
                DBObject clause1 = new BasicDBObject("nl_uri", subject.toString());
                DBObject clause2 = new BasicDBObject("en_uri", object.toString());    
                BasicDBList or = new BasicDBList();
                or.add(clause1);
                or.add(clause2);

                entry = new BasicDBObject().append("$set", new BasicDBObject().append("nl_uri", subject.toString()).append("en_uri", object.toString()));
                MongoDBClient.getDBInstance(dbName).getCollection("interlanguage_links").update(
                        new BasicDBObject("$or", or), entry, true, false);
                
            } else if(object.toString().startsWith("http://de.dbpedia.org")) {
                
                DBObject clause1 = new BasicDBObject("nl_uri", subject.toString());
                DBObject clause2 = new BasicDBObject("de_uri", object.toString());    
                BasicDBList or = new BasicDBList();
                or.add(clause1);
                or.add(clause2);

                entry = new BasicDBObject().append("$set", new BasicDBObject().append("nl_uri", subject.toString()).append("de_uri", object.toString()));
                MongoDBClient.getDBInstance(dbName).getCollection("interlanguage_links").update(
                        new BasicDBObject("$or", or), entry, true, false);
            }            
        }
        System.out.println("Finished inserting NL inter-language links.");        
    }

    private void loadDbpediaInstanceTypesEN() {
        
        Model model = ModelFactory.createDefaultModel();
        InputStream in = FileManager.get().open( enDbpediaInstances );
        model.read(in, null, "N-TRIPLE");
        
        StmtIterator iter = model.listStatements( new SimpleSelector(null, null,  (RDFNode)null));
        
        while (iter.hasNext()) {
            
            Statement stm = iter.next();
            RDFNode subject = stm.getSubject();
            RDFNode object = stm.getObject();
            
            String[] sub = subject.toString().split("/");
            String entity = sub[sub.length-1].replace("_", " ");
            
            String[] obj = object.toString().split("/");
            String hypernym = obj[obj.length-1].replace("_", " ");
            
            BasicDBObject entityType = new BasicDBObject().append("$push",
                                    new BasicDBObject().append("types", new BasicDBObject()
                                        .append("label", hypernym)
                                        .append("uri", object.toString())
                                        .append("origin", "dbpedia")
                                        .append("mapping","n/a")));

            MongoDBClient.getDBInstance(dbName).getCollection("en_entities_dbpedia").update(
                            new BasicDBObject().append("label", entity).append("uri", subject.toString()), entityType, true, false);        
            
        }
        System.out.println("Finished inserting EN dbpedia instances");
    }
    
    private void loadDbpediaInstanceTypesDE() {
        
        Model model = ModelFactory.createDefaultModel();
        InputStream in = FileManager.get().open( deDbpediaInstances);
        model.read(in, null, "N-TRIPLE");
        
        StmtIterator iter = model.listStatements( new SimpleSelector(null, null,  (RDFNode)null));
        
                
        while (iter.hasNext()) {
            Statement stm = iter.next();
            RDFNode subject = stm.getSubject();
            RDFNode object = stm.getObject();
            
            String[] sub = subject.toString().split("/");
            String entity = sub[sub.length-1].replace("_", " ");
            
            String[] obj = object.toString().split("/");
            String hypernym = obj[obj.length-1].replace("_", " ");
            
            BasicDBObject entityType = new BasicDBObject().append("$push",
                                    new BasicDBObject().append("types", new BasicDBObject()
                                        .append("label", hypernym)
                                        .append("uri", object.toString())
                                        .append("origin", "dbpedia")
                                        .append("mapping","n/a")));

            MongoDBClient.getDBInstance(dbName).getCollection("de_entities_dbpedia").update(
                            new BasicDBObject().append("label", entity).append("uri", subject.toString()), entityType, true, false);        
        }
        System.out.println("Finished inserting DE dbpedia instances");
    }
    
    private void loadDbpediaInstanceTypesNL() {
        
        Model model = ModelFactory.createDefaultModel();
        InputStream in = FileManager.get().open( nlDbpediaInstances );
        model.read(in, null, "N-TRIPLE");
        
        StmtIterator iter = model.listStatements( new SimpleSelector(null, null,  (RDFNode)null));
        
        while (iter.hasNext()) {
            Statement stm = iter.next();
            RDFNode subject = stm.getSubject();
            RDFNode object = stm.getObject();
            
            String[] sub = subject.toString().split("/");
            String entity = sub[sub.length-1].replace("_", " ");
            
            String[] obj = object.toString().split("/");
            String hypernym = obj[obj.length-1].replace("_", " ");
            
            BasicDBObject entityType = new BasicDBObject().append("$push",
                                    new BasicDBObject().append("types", new BasicDBObject()
                                        .append("label", hypernym)
                                        .append("uri", object.toString())
                                        .append("origin", "dbpedia")
                                        .append("mapping","n/a")));

            MongoDBClient.getDBInstance(dbName).getCollection("nl_entities_dbpedia").update(
                            new BasicDBObject().append("label", entity).append("uri", subject.toString()), entityType, true, false);        
        }
        System.out.println("Finished inserting NL dbpedia instances");
       
    }  
    
    private void loadYAGOENLabels() {
        
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(yagoLabels));
            String line;
            String[] stm;
            int step = 0;
            int counter = 0;

            while ((line = br.readLine()) != null) {

                counter ++;
                step++;
                if(step == 10000){
                    System.out.println(counter);
                    step=0;
                }
                
                stm = line.split("\\t");
                // http://yago-knowledge.org/resource/Eduard_Telcs
                if(stm[2].equals("rdfs:label")){
                    
                    if(stm[3].endsWith("\"@eng")) {
                        ////System.out.println("subj: " + stm[1].substring(1, stm[1].length()-1) + " , obj: " + stm[3].substring(1, stm[3].length()-5));
                        BasicDBObject label = new BasicDBObject().append("$push",
                                    new BasicDBObject().append("labels", new BasicDBObject()
                                        .append("label",  stm[3].substring(1, stm[3].length()-5))
                                        .append("lang", "en")));
                        MongoDBClient.getDBInstance(dbName).getCollection("entities_yago").update(
                                new BasicDBObject().append("uri", "http://yago-knowledge.org/resource/" + stm[1].substring(1, stm[1].length()-1)),
                                label, true, false);
                    } else {
//                        //System.out.println("subj: " + stm[1] + " , obj: " + stm[3].substring(1, stm[3].length()-1));  
                        BasicDBObject label = new BasicDBObject().append("$push",
                                    new BasicDBObject().append("labels", new BasicDBObject()
                                        .append("label",  stm[3].substring(1, stm[3].length()-1))
                                        .append("lang", "en")));
                        MongoDBClient.getDBInstance(dbName).getCollection("entities_yago").update(
                                new BasicDBObject().append("uri", "http://yago-knowledge.org/resource/" + stm[1].substring(1, stm[1].length()-1)),
                                label, true, false);
                    }
                }
            }
            //System.out.println(counter);
            br.close();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(RDFLoader.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(RDFLoader.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                br.close();
            } catch (IOException ex) {
                Logger.getLogger(RDFLoader.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        System.out.println("Yago EN labels was successfully loaded");
    }    

    private void loadYAGOMultilingualLabels() {
        
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(yagoMultilanguageLabels));
            String line;
            String[] stm;
            int step = 0;
            int counter = 0;
            
            while ((line = br.readLine()) != null) {
                
                counter ++;
                step++;
                if(step == 10000){
                    System.out.println(counter);
                    step=0;
                }

                stm = line.split("\\t");
                   
                if(stm[3].endsWith("\"@deu")) {
                    ////System.out.println("subj: " + stm[1].substring(1, stm[1].length()-1) + " , obj: " + stm[3].substring(1, stm[3].length()-5));
                    BasicDBObject label = new BasicDBObject().append("$push",
                                    new BasicDBObject().append("labels", new BasicDBObject()
                                        .append("label",  stm[3].substring(1, stm[3].length()-5))
                                        .append("lang", "de")));
                    
                    MongoDBClient.getDBInstance(dbName).getCollection("entities_yago").update(
//                            new BasicDBObject().append("uri", "http://de.dbpedia.org/resource/"+stm[1].substring(1, stm[1].length()-1)),                            
                            new BasicDBObject().append("uri", "http://yago-knowledge.org/resource/"+stm[1].substring(1, stm[1].length()-1)),                            
                                label, true, false);
                }
            }
            br.close();            
        } catch (FileNotFoundException ex) {
            Logger.getLogger(RDFLoader.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(RDFLoader.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                br.close();
            } catch (IOException ex) {
                Logger.getLogger(RDFLoader.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        System.out.println("Yago multilangual labels was successfully loaded");
    }
    
    private void loadYAGOTypes() {
        BufferedReader br = null;
        
            int counter = 0;
        try {
            br = new BufferedReader(new FileReader(yagoTypes));
            String line;
            String[] stm;
            int step = 0;
            while ((line = br.readLine()) != null) {
                
                counter ++;
                step++;
                if(step == 10000){
                    System.out.println(counter);
                    step=0;
                }
//                System.out.println(counter);
                stm = line.split("\\t");
                
                if(stm[2].equals("rdf:type")){
//                    String s1 = stm[3].substring(14, stm[3].length()-1).replaceAll("_", " ");
//                    String s2 = stm[3].substring(1, stm[3].length()-1);
                    
                    BasicDBObject entityType = new BasicDBObject().append("$push",
                                    new BasicDBObject().append("types", new BasicDBObject()
                                        .append("label", stm[3].substring(14, stm[3].length()-1).replaceAll("_", " "))
                                        .append("uri", "http://yago-knowledge.org/resource/" + stm[3].substring(1, stm[3].length()-1))));
//                                        .append("uri", "http://yago-knowledge.org/resource/" + stm[3].substring(1, stm[3].length()-1))));
                        
                    MongoDBClient.getDBInstance(dbName).getCollection("entities_yago").update(
                            new BasicDBObject().append("uri", "http://yago-knowledge.org/resource/" + stm[1].substring(1, stm[1].length()-1) ), entityType, true, false);
//                            new BasicDBObject().append("uri", "http://yago-knowledge.org/resource/"+stm[1].substring(1, stm[1].length()-1) ), entityType, true, false);
                }
            }
            br.close();            
        } catch (StringIndexOutOfBoundsException ex) {
            
            System.out.println("problem: "+ counter);
            Logger.getLogger(RDFLoader.class.getName()).log(Level.SEVERE, null, ex);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(RDFLoader.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(RDFLoader.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                br.close();
            } catch (IOException ex) {
                Logger.getLogger(RDFLoader.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        System.out.println("Yago EN types was successfully loaded");
    }    
}
