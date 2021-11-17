package bd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.StringTokenizer;

import javax.naming.LimitExceededException;

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
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;

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

		int i = 0;

		// Démarrer une session
		Session session = driver.session();

		MongoDatabase mongoDb = mongoClient.getDatabase("dbDocuments");

		// Suppression collections
		MongoCollection<Document> indexColl = mongoDb.getCollection("index");
		/*indexColl.drop();
		indexColl = mongoDb.getCollection("indexInverse");
		indexColl.drop();

		indexColl = mongoDb.getCollection("index");
		StatementResult res = session.run("match (n:Article) return n.titre, id(n)");
		for (Record r : res.list()) {
			int idArticle = r.get("id(n)").asInt();
			String titreArticle = r.get("n.titre").asString();
			StringTokenizer st = new StringTokenizer(titreArticle.toLowerCase(), ",'-:;.()+[]{}?! ");
			BasicBSONList motCle = new BasicBSONList();
			doc = new Document("idDocument", idArticle);
			while (st.hasMoreTokens()) {
				// motCle.add(st.nextToken());
				motCle.add(st.nextToken());
				i++;
			}
			// if(!motCle.isEmpty()) {
			doc.append("motsCle", motCle);
			// }
			// System.out.println(doc);
			indexColl.insertOne(doc);
//			System.out.println(doc);
		}

		// créer index
		indexColl.createIndex(Indexes.ascending("motsCle"));

		// Affichage de tous les documents via un itérateur
		FindIterable<Document> documents = indexColl.find();
		MongoCollection<Document> indexInverseColl = mongoDb.getCollection("indexInverse");
		for (MongoCursor<Document> it = documents.iterator(); it.hasNext();) {
			Document d = it.next();
			// System.out.println(d.get("motsCle").toString());
			// System.out.println(d.get("motsCle").getClass());
			ArrayList listeMotsCles = (ArrayList) d.get("motsCle");
			// pour chaque mot clé
			for (Object o : listeMotsCles) {
				String m = o.toString();
				// rechercher un mot déjà existant dans indexInverse
				FindIterable<Document> value = indexInverseColl.find(Filters.eq("mot", m));
				// si mot déjà existant
				if (value.first() != null) {
					// créer doc
					Document dex = (Document) value.first();
					// rajouter un id doc
//					dex.append("documents", d.get("idDocument"));
//					dex.put("documents", d.get("idDocument"));
					// remplacer l'ancien doc par le doc mis à jour
					mongoDb.getCollection("indexInverse").updateOne(Filters.eq("_id", dex.get("_id")),
							Updates.addToSet("documents", d.get("idDocument")));
					// si mot n'existait pas encore dans indexInverse
				} else {
					// créer doc
					Document newDoc = new Document("mot", m);
					List<Integer> listeIdDocs = new ArrayList<>();
					listeIdDocs.add(d.getInteger("idDocument"));
					// ajouter une liste d'id
//					newDoc.append("documents", listeIdDocs);
					newDoc.put("documents", listeIdDocs);
					indexInverseColl.insertOne(newDoc);
				}
			}
		}

		// créer index
		indexInverseColl.createIndex(Indexes.ascending("mot"));


		MongoCollection<Document> indexInverseColl = mongoDb.getCollection("indexInverse");
		for (MongoCursor<Document> d = indexInverseColl.find(Filters.eq("mot", "with")).iterator(); d.hasNext();) {
			List<Integer> listeDocs = new ArrayList<>();
			listeDocs = (List<Integer>) d.next().get("documents");
			StatementResult res = session.run("MATCH (n:Article) " + 
					"where id(n) in "+listeDocs+ " return n.titre order by n.titre asc");
			for (Record r : res.list()) {
				System.out.println(r.get("n.titre").toString());
			}
		}
		
		System.out.println("QUESTION 3.5");
		StatementResult res = session.run("MATCH (n:Article)<-[r:Ecrire]-(a:Auteur) " + 
				"return a.nom, count(n) as nbTitres " + 
				"order by nbTitres desc, a.nom asc " + 
				"limit 10");
		for (Record r : res.list()) {
			System.out.println(r.get("nbTitres") + " - " + r.get("a.nom").toString().replace("\"", ""));
		}*/

		System.out.println("QUESTION 3.6");
		MongoCollection<Document> indexInverseColl = mongoDb.getCollection("indexInverse");
		List<String> listeValeursPossibles = new ArrayList<String>();
		listeValeursPossibles.add("with");
		listeValeursPossibles.add("systems");
		listeValeursPossibles.add("new");
		AggregateIterable<Document> listeDocCherches = indexInverseColl.aggregate(
				Arrays.asList(
						Aggregates.match(Filters.in("mot", listeValeursPossibles)),
						Aggregates.unwind("$documents"),
						Aggregates.group("$documents", Accumulators.sum("nb", 1)),
						Aggregates.sort(Sorts.descending("nb")),
						Aggregates.limit(10)
				));
		
		List<Integer> listeDocs = new ArrayList<>();
		for (Document d : listeDocCherches) {
//			System.out.println("mon doc : " + d.toString());
			StatementResult res = session.run("MATCH (n:Article) " + 
					"where id(n) = "+ d.getInteger("_id")+ " return id(n), n.titre order by n.titre asc " + 
					"limit 10");
			for (Record r : res.list()) {
				System.out.println(r.get("id(n)") + " " + r.get("n.titre").toString().replace("\"", "") + " " + d.get("nb"));
			}
		}
		

		
		// Fermeture
		session.close();
		mongoClient.close();
		driver.close();

	}

}
