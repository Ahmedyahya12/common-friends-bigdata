// Question 1 : Générer les paires
def pairs(str: Array[String]) = {
  val users = str(1).split(",")
  val user = str(0)
  val n = users.length
  for (i <- 0 until n) yield {
    val pair = if (user < users(i)) (user, users(i)) else (users(i), user)
    (pair, users)
  }
}

// Question 2 : Traitement principal
val data = sc.textFile("file:///C:/SparkTP/MutualFriends/soc-LiveJournal1Adj.txt")
val data1 = data.map(x => x.split("\t")).filter(li => li.size == 2)
val pairCounts = data1.flatMap(pairs).reduceByKey((list1, list2) => list1.intersect(list2))

val p1 = pairCounts.map { case ((user1, user2), mutual) =>
  val cleanMutual = mutual.filter(_.nonEmpty)
  s"$user1\t$user2\t${cleanMutual.mkString(",")}"
}

// Afficher un aperçu dans le terminal
println("=== Aperçu des 10 premières paires avec leurs amis mutuels ===")
p1.take(10).foreach(println)
println("=============================================================\n")

// Enregistrer toutes les paires avec amis communs
p1.saveAsTextFile("output")  // ✅ Dossier de sortie global

// Question 3 : Extraire les paires spécifiques
def afficherAmisCommun(userA: String, userB: String): String = {
  val amis = p1
    .map(_.split("\t"))
    .filter(x => x.length == 3 && ((x(0) == userA && x(1) == userB) || (x(0) == userB && x(1) == userA)))
    .flatMap(x => x(2).split(",").filter(_.nonEmpty))
    .collect()

  val result =
    if (amis.isEmpty)
      s"$userA\t$userB\tAucun ami mutuel"
    else
      s"$userA\t$userB\t${amis.mkString(",")}"

  println("----- " + result + " -----\n")
  result
}

// Extraire et afficher les 5 paires demandées
val results = Seq(
  afficherAmisCommun("0", "4"),
  afficherAmisCommun("20", "22939"),
  afficherAmisCommun("1", "29826"),
  afficherAmisCommun("6222", "19272"),
  afficherAmisCommun("28041", "28056")
)

// Enregistrer les 5 paires spécifiques dans un fichier
val output1 = sc.parallelize(results)
output1.saveAsTextFile("output1")  // ✅ Dossier de sortie spécifique

println("=== Fin des résultats des questions spécifiques ===")
