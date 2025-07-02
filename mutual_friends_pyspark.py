from pyspark import SparkContext

# 1. Créer le SparkContext
sc = SparkContext("local", "MutualFriends")

# 2. Charger le fichier
rdd = sc.textFile("file:///C:/SparkTP/MutualFriends/friends_common.txt")

# 3. Supprimer la ligne de commentaire (entête)
rdd_clean = rdd.filter(lambda line: not line.startswith("#"))

# 4. Transformer en (id, nom, [amis])
def parse_line(line):
    parts = line.strip().split()
    user_id = parts[0]
    name = parts[1]
    friends = parts[2].split(",") if len(parts) > 2 else []
    return (user_id, name, friends)

parsed = rdd_clean.map(parse_line)

# 5. Créer les paires (id1, id2) → (set d'amis)
def generate_pairs(user_id, friends):
    pairs = []
    for friend in friends:
        small, big = sorted([user_id, friend])
        pairs.append(((small, big), set(friends)))
    return pairs

pairs = parsed.flatMap(lambda x: generate_pairs(x[0], x[2]))

# 6. Intersection pour obtenir amis communs
mutual_friends = pairs.reduceByKey(lambda a, b: a.intersection(b))

# 7. Récupérer dictionnaire des noms
names_dict = parsed.map(lambda x: (x[0], x[1])).collectAsMap()

# 8. Fonction pour afficher les amis communs entre deux noms
def find_common(nomA, nomB):
    idA = None
    idB = None

    # Rechercher les IDs des noms
    for id_, name in names_dict.items():
        if name == nomA:
            idA = id_
        if name == nomB:
            idB = id_

    if not idA or not idB:
        print("Utilisateur introuvable")
        return

    key = tuple(sorted([idA, idB]))
    results = mutual_friends.lookup(key)

    if results:
        common_ids = list(results[0])
        common_names = [names_dict.get(fid, fid) for fid in common_ids]
        print(f"\n{nomA} <--> {nomB} : Amis communs : {', '.join(common_names)}")
    else:
        print(f"\n{nomA} <--> {nomB} : Aucun ami commun")

# 9. Appels pour les paires spécifiques
find_common("Sidi", "Mohamed")
find_common("Aicha", "Ahmed")
find_common("Mohamed", "Leila")

sc.stop()
