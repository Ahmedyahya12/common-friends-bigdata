// Charger le fichier
val data = sc.textFile("file:///C:/SparkTP/MutualFriends/friends_common.txt")

// Supprimer la ligne de titre
val clean = data.filter(line => !line.startsWith("#"))

// Parser chaque ligne : (id, nom, [amis])
val parsed = clean.map(line => {
  val parts = line.trim.split("\\s+")
  val id = parts(0)
  val name = parts(1)
  val friends = if (parts.length > 2) parts(2).split(",").toList else List()
  (id, name, friends)
})

// Créer une map : ID -> Nom
val idToName = parsed.map(x => (x._1, x._2)).collect().toMap

// Générer les couples (id1, id2) → liste des amis
val pairs = parsed.flatMap { case (id, name, friends) =>
  friends.map(fid => {
    val pair = if (id < fid) (id, fid) else (fid, id)
    (pair, friends)
  })
}

// Calcul des amis communs
val mutualFriends = pairs.reduceByKey((a, b) => a.intersect(b))

// Fonction pour afficher amis communs par noms
def afficherAmisCommuns(nomA: String, nomB: String): Unit = {
  val ids = idToName.filter { case (id, name) => name == nomA || name == nomB }.keys.toList
  if (ids.length != 2) {
    println("Noms introuvables")
    return
  }
  val pair = if (ids(0) < ids(1)) (ids(0), ids(1)) else (ids(1), ids(0))
  val result = mutualFriends.lookup(pair)
  if (result.nonEmpty) {
    val amis = result(0).filter(idToName.contains).map(idToName)
    println(s"$nomA <--> $nomB : ${amis.mkString(", ")}")
  } else {
    println(s"$nomA <--> $nomB : Aucun ami commun")
  }
}

// Exemples d'utilisation :
afficherAmisCommuns("Sidi", "Mohamed")
afficherAmisCommuns("Aicha", "Ahmed")
afficherAmisCommuns("Mohamed", "Leila")
