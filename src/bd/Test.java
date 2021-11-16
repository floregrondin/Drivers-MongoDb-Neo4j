package bd;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.BasicBSONList;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;

public class Test {

	public static void main(String[] args) {
		// pour la bd neo4j
		Driver driver = GraphDatabase.driver("bolt://192.168.56.50");
		
		// pour la bd mongodb
		String uri = "mongodb://192.168.56.50:27017";
		MongoClientURI connectionString = new MongoClientURI(uri);
		// Connexion au serveur MongDB via la connectString
		MongoClient mongoClient = new MongoClient(connectionString);
		
		Document doc = null;
		
		int i=0;
		
		// Démarrer une session
		Session session = driver.session();
		
		MongoDatabase mongoDb = mongoClient.getDatabase("dbDocuments");
		
		//Suppression collections
		MongoCollection<Document> indexColl = mongoDb.getCollection("index");
		indexColl.drop();
		indexColl = mongoDb.getCollection("indexInverse");
		indexColl.drop();
		
		indexColl = mongoDb.getCollection("index");
		StatementResult res = session.run( "match (n:Article) return n.titre, id(n)");
		for (Record r : res.list()){
			int idArticle = r.get("id(n)").asInt();
			String titreArticle = r.get("n.titre").asString();
			StringTokenizer st = new StringTokenizer (titreArticle.toLowerCase(), ",'-:;.()+[]{}?! ");
			BasicBSONList motCle = new BasicBSONList();
			doc = new Document("idDocument", idArticle);
			while (st.hasMoreTokens()){ 
				//motCle.add(st.nextToken());
				motCle.add( st.nextToken());
				i++;
			}
			//if(!motCle.isEmpty()) {
				doc.append("motsCle", motCle);
			//}
			//System.out.println(doc);
			indexColl.insertOne(doc);
			System.out.println(doc);
		}
		
		//créer index
		indexColl.createIndex(Indexes.ascending("motsCle"));
		
		// Affichage de tous les documents via un itérateur
		FindIterable<Document> documents = indexColl.find();
		MongoCollection<Document> indexInverseColl = mongoDb.getCollection("indexInverse");
		for (MongoCursor<Document> it = documents.iterator(); it.hasNext();) {
			Document d = it.next();
			System.out.println(d.get("motsCle").toString());
			System.out.println(d.get("motsCle").getClass());
			ArrayList listeMotsCles = (ArrayList) d.get("motsCle");
			// pour chaque mot clé
			for (Object o : listeMotsCles) {
				String m  = o.toString();
				//rechercher un mot déjà existant dans indexInverse
				FindIterable<Document> value = indexInverseColl.find(Filters.eq("mot", m));
				//si mot déjà existant
				if (value.first() != null) {
					//créer doc
					Document dex = (Document) value.first();
					//rajouter un id doc
//					System.out.println("mon doc : " + dex);
//					System.out.println("idDoc : " + d.get("idDocument"));
					dex.append("documents", d.get("idDocument"));
					//remplacer l'ancien doc par le doc mis à jour
					mongoDb.getCollection("indexInverse").replaceOne(Filters.eq("_id", dex.get("_id")), dex);
				//si mot n'existait pas encore dans indexInverse
				} else {
					//créer doc 
					Document newDoc = new Document("mot",m);
					List<String> listeIdDocs = new ArrayList<>();
					listeIdDocs.add(d.get("idDocument").toString());
					//ajouter une liste d'id
					newDoc.append("documents", listeIdDocs);
					indexInverseColl.insertOne(newDoc);
				}
			}
		}

		//créer index
		indexInverseColl.createIndex(Indexes.ascending("mot"));
		
		//rechercher mot
		//pour filtrer title asc dans index
		Bson sort = Sorts.ascending("title");
		//Bson project = fields.
		
		indexInverseColl.find(Filters.eq("mot", "with"));
		
		
		//Fermeture
		session.close();
		mongoClient.close();
		driver.close();
		
	}
	
}
