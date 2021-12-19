package bd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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

		// INITIALISATION
		// Suppression des collections si elles existaient déjà
		MongoCollection<Document> indexColl = mongoDb.getCollection("index");
		indexColl.drop();
		indexColl = mongoDb.getCollection("indexInverse");
		indexColl.drop();
		
		// Création de la collection 'index'
		indexColl = mongoDb.getCollection("index");
		StatementResult res = session.run("match (n:Article) return n.titre, id(n)");
		// Pour chaque res de notre req Cypher
		for (Record r : res.list()) {
			// Récupérer l'id de l'article
			int idArticle = r.get("id(n)").asInt();
			// Récupérer le titre de l'article
			String titreArticle = r.get("n.titre").asString();
			// Isoler les mots-clés depuis le titre de l'article
			StringTokenizer st = new StringTokenizer(titreArticle.toLowerCase(), ",'-:;.()+[]{}?! ");
			BasicBSONList motCle = new BasicBSONList();
			doc = new Document("idDocument", idArticle);
			while (st.hasMoreTokens()) {
				motCle.add(st.nextToken());
				i++;
			}
			// Ajouter les mots-clés
			doc.append("motsCle", motCle);
			// Créer le document
			indexColl.insertOne(doc);
		}

		// créer index
		indexColl = mongoDb.getCollection("index");
		indexColl.createIndex(Indexes.ascending("motsCle"));

		// Affichage de tous les documents de notre collection index via un itérateur
		FindIterable<Document> documents = indexColl.find();
		// Créer une collection indexInverse
		MongoCollection<Document> indexInverseColl = mongoDb.getCollection("indexInverse");
		for (MongoCursor<Document> it = documents.iterator(); it.hasNext();) {
			Document d = it.next();
			ArrayList listeMotsCles = (ArrayList) d.get("motsCle");
			// Pour chaque mot clé
			for (Object o : listeMotsCles) {
				String m = o.toString();
				// Rechercher un mot déjà existant dans indexInverse
				FindIterable<Document> value = indexInverseColl.find(Filters.eq("mot", m));
				// Si mot déjà existant
				if (value.first() != null) {
					// Créer doc
					Document dex = (Document) value.first();
					// Remplacer l'ancien doc par le doc mis à jour
					mongoDb.getCollection("indexInverse").updateOne(Filters.eq("_id", dex.get("_id")),
							Updates.addToSet("documents", d.get("idDocument")));
				// Si mot n'existait pas encore dans indexInverse
				} else {
					// Créer doc
					Document newDoc = new Document("mot", m);
					List<Integer> listeIdDocs = new ArrayList<>();
					listeIdDocs.add(d.getInteger("idDocument"));
					// Ajouter une liste d'id
					newDoc.put("documents", listeIdDocs);
					indexInverseColl.insertOne(newDoc);
				}
			}
		}

		// créer index
		indexInverseColl = mongoDb.getCollection("indexInverse");
		indexInverseColl.createIndex(Indexes.ascending("mot"));

		for (MongoCursor<Document> d = indexInverseColl.find(Filters.eq("mot", "with")).iterator(); d.hasNext();) {
			List<Integer> listeDocs = new ArrayList<>();
			listeDocs = (List<Integer>) d.next().get("documents");
			res = session.run("MATCH (n:Article) " + 
					"where id(n) in "+listeDocs+ " return n.titre order by n.titre asc");
			for (Record r : res.list()) {
				System.out.println(r.get("n.titre").toString());
			}
		}


		System.out.println("QUESTION 3.5");
		res = session.run("MATCH (n:Article)<-[r:Ecrire]-(a:Auteur) " + 
				"return a.nom, count(n) as nbTitres " + 
				"order by nbTitres desc, a.nom asc " + 
				"limit 10");
		for (Record r : res.list()) {
			System.out.println(r.get("nbTitres") + " - " + r.get("a.nom").toString().replace("\"", ""));
		}
		 

		System.out.println("QUESTION 3.6");
		// Saisir en dur les valeurs recherchées
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

		// Pour stocker les id des enregistrements ayant un même nb d'occurences pour les mots-clés recherchés 
		List<Integer> listeId = new ArrayList<>();
		// Pour stocker id de l'article et nb d'occurences des mots-clés recherchés
		HashMap<Integer, String> listeIdDoc = new HashMap<>();
		// Incrémenté dès qu'un doc a été traité, pour aller jusqu'à 10
		int indice = 0;
		String nbDocTmp = null;
		// Pour chaque doc récupéré de Mongodb
		for (Document d : listeDocCherches) {
			// Créer un indice
			indice++;
			// A partir du 2e enregistrement et si le nb d'occurences pour cet enregistrement n'est pas le même qu'au précédent
			if (nbDocTmp != null && !nbDocTmp.equals(d.get("nb").toString())) {
				res = session.run("MATCH (n:Article) " + 
						"where id(n) in "+ listeId + " return id(n), n.titre order by n.titre asc " + 
						"limit 10");
				for (Record r : res.list()) {
					System.out.println(r.get("id(n)") + " " + r.get("n.titre").toString().replace("\"", "") + " " + 
							listeIdDoc.get(r.get("id(n)").asInt()));
				}
				listeId = new ArrayList<>();
			}
			// Stocker le nb d'occurences
			nbDocTmp = d.get("nb").toString();
			res = session.run("MATCH (n:Article) " + 
					"where id(n) = "+ d.getInteger("_id")+ " return id(n), n.titre " + 
					"limit 10");
			for (Record r : res.list()) {
				listeId.add(r.get("id(n)").asInt());
				listeIdDoc.put(r.get("id(n)").asInt(), d.get("nb").toString());
			}
			
			// Si on est à 10 alors on a parcouru tous les docs
			if(indice == 10) {
				// On cherche l'article correspondant
				res = session.run("MATCH (n:Article) " + 
						"where id(n) in "+ listeId + " return id(n), n.titre order by n.titre asc " + 
						"limit 10");
				// Affichage pour chaque enregistrement
				for (Record r : res.list()) {
					System.out.println(r.get("id(n)") + " " + r.get("n.titre").toString().replace("\"", "") + " " + 
							listeIdDoc.get(r.get("id(n)").asInt()));
				}
			}
		}

	// Fermeture
	session.close();
	mongoClient.close();
	driver.close();

}

}
