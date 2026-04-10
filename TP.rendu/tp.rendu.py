#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 18 16:35:26 2023

@author: vlemeur
"""

from pymongo import MongoClient
from datetime import datetime
import pprint
import matplotlib.pyplot as plt
client = MongoClient(host="localhost", port=27017)

db = client["tp"]

# Info sur le resto

cursor = db.restaurants.find({"name": "Maracas Steakhouse"})
l_resultat2_1 = list(cursor)
drc_resultat2_1=l_resultat2_1[0]


pprint.pprint(drc_resultat2_1)

 # 'address': {'building': '143',
 #             'coord': [-73.9234825, 40.8643445],
 #             'street': 'Sherman Avenue',
 #             'zipcode': '10034'},
 # 'borough': 'Manhattan',
 # 'cuisine': 'Spanish',
 # 'grades': [{'date': datetime.datetime(2014, 7, 25, 0, 0),
 #             'grade': 'A',
 #             'score': 12},
 #            {'date': datetime.datetime(2014, 2, 28, 0, 0),
 #             'grade': 'A',
 #             'score': 4},
 #            {'date': datetime.datetime(2013, 8, 6, 0, 0),
 #             'grade': 'A',
 #             'score': 5},
 #            {'date': datetime.datetime(2012, 11, 15, 0, 0),
 #             'grade': 'A',
 #             'score': 13},
 #            {'date': datetime.datetime(2012, 6, 14, 0, 0),
 #             'grade': 'B',
 #             'score': 15},
 #            {'date': datetime.datetime(2011, 11, 19, 0, 0),
 #             'grade': 'A',
 #             'score': 12}],
 # 'name': 'Maracas Steakhouse',
 # 'restaurant_id': '41242602'}


#  Les notes du restaurant

# la moyenne
id='41242602'
cuisine='Spanish'
borough='Manhattan'
cursor=db.restaurants.aggregate([
    {
     "$match":{"restaurant_id" : id}
     },
    {
        "$unwind": "$grades"  
    },
    {
        "$project" : {
            "score" : "$grades.score",
            "grade" : "$grades.grade",
        }
    },
    {
        "$group": {
            "_id": None,
            "moy": {"$avg": "$score"}
        }
    },
    {
        "$project" : {
            "_id":0,
            "moyenne": "$moy"
        }
    },
    ])
l_resultat2 = list(cursor)
drc_resultat2=l_resultat2[0]

pprint.pprint(drc_resultat2)

# Afficher les scores en fonction du temps:
    
cursor = db.restaurants.aggregate([{"$match":{"restaurant_id": id}},
                                   
                                   {"$project":{"grades.date":1,
                                                "grades.score":1,
                                                "_id":0
                                                }
                                    },
                                    ])
l_resultat3 = list(cursor)
drc_resultat3=l_resultat3[0]


dates_list = [grade.get("date") for grade in drc_resultat3.get("grades", [])]
scores_list = [grade.get("score") for grade in drc_resultat3.get("grades", [])]


plt.plot(dates_list,scores_list)
plt.show()

print()
print("Les restaurants Spanish dans la même ville: ")

cursor = db.restaurants.aggregate([
    { 
     "$match": {"cuisine": cuisine,
                }
     },
    {
     "$group":{ "_id": None,
               "nbr_restaurant":{"$sum":1}}
     },
    {
     "$project": {"_id":0,
                  }
     }
])

result = list(cursor)
drc_resultat=result[0]

print()
pprint.pprint(drc_resultat)


print()
print("les restaurants spanish dans le même quartier:")
print()

    
cursor = db.restaurants.aggregate([
    { 
     "$match": {"cuisine": cuisine,
                "borough": borough
                }
     },
    {
     "$group":{ "_id": None,
               "nbr_restaurant":{"$sum":1}}
     },
    {
     "$project": {"_id":0,
                  }
     }
])

result = list(cursor)
drc_resultat=result[0]

pprint.pprint(drc_resultat)
print()
print("Les restaurants Spanish dans la même rue:")
print()
cursor = db.restaurants.aggregate([
    {
        "$match": {"cuisine": cuisine,
                   "address.street": "Sherman Avenue"}
    },
    {
     "$project": {"_id":0,
                  "name":1,
                  "rue":"$address.street",
                  "cuisine":1}
     },
    {"$sort":{ "name":1}}
    ,{
        "$project": {
            "_id":0,
            "rue": 0,
            }
    }
])

result = list(cursor)
pprint.pprint(result)

print()
print("La moyenne des scores des retaurants de la rue")
print()

cursor = db.restaurants.aggregate([
     
    {
        "$match": {"address.street": "Sherman Avenue"}
    },
   
    {
        "$unwind": "$grades"
    },
   
    {
        "$project": {
            "_id": 0,
            "name": 1,
            "score": "$grades.score"
        }
    },
    
    {
        "$group": {
            "_id": "$name",
            "moyenne": {"$avg": "$score"}
        }
    },
    
    {"$sort": {"moyenne": 1}}
])

# Récupère le résultat sous forme de liste et l'affiche joliment
result = list(cursor)

for restaurant in result:
    pprint.pprint({"name": restaurant["_id"], "moyenne": restaurant["moyenne"]})

print()
print("La moyenne des restaurants Spanish de la rue")
print()

cursor = db.restaurants.aggregate([
     
    {
        "$match": {"cuisine": cuisine,
                   "address.street": "Sherman Avenue"}
    },
   
    {
        "$unwind": "$grades"
    },
   
    {
        "$project": {
            "_id": 0,
            "name": 1,
            "score": "$grades.score"
        }
    },
    
    {
        "$group": {
            "_id": "$name",
            "moyenne": {"$avg": "$score"}
        }
    },
    
    {"$sort": {"moyenne": 1}}
])

# Récupère le résultat sous forme de liste et l'affiche joliment
result = list(cursor)

for restaurant in result:
    pprint.pprint({"name": restaurant["_id"], "moyenne": restaurant["moyenne"]})

print()
print("Compte le nombre de restaurants spanish pour chaque quartier")
print()



cursor = db.restaurants.aggregate([
    {
        "$match": {"cuisine": "Spanish"}
    },
    {
        "$group": {
            "_id": "$borough",
            "count": {"$sum": 1}
        }
    },
    {
        "$project": {
            "_id": 0,
            "borough": "$_id",
            "count": 1
        }
    },
    {
        "$sort": {"count": 1}
    }
])

# Récupère le résultat sous forme de liste et l'affiche joliment
result = list(cursor)
pprint.pprint(result)

print()
print("La moyenne des restaurants Spanish pour chaques quartier")
print()

cursor = db.restaurants.aggregate([
    {
        "$match": {"cuisine": "Spanish"}
    },
    {
        "$unwind": "$grades"
    },
    {
        "$project": {
            "_id": 0,
            "name": 1,
            "borough": 1,
            "score": "$grades.score"
        }
    },
    {
        "$group": {
            "_id": "$borough",
            "moyenne": {"$avg": "$score"}
        }
    },
    {
        "$sort": {"moyenne": 1}
    },
    {
        "$project": {
            "_id": 0,
            "borough": "$_id",
            "moyenne": 1
        }
    }
])

# Récupère le résultat sous forme de liste et l'affiche joliment
result = list(cursor)
pprint.pprint(result)

print()
print("Le restaurant Spanish le mieux noté de la ville (avec au moins 3 scores):")
print()




cursor = db.restaurants.aggregate([
    {
        "$match": {"cuisine": "Spanish"}
    },
    {
        "$unwind": "$grades"
    },
    {
        "$group": {
            "_id": "$name",
            "moyenne": {"$avg": "$grades.score"},
            "count":{"$sum":1}
        }
    },
    {
     "$match": {"count": {"$gte":3}}
     },
    {
        "$sort": {"moyenne": -1}
    },
    {
        "$limit": 1
    },
    {
        "$project": {
            "_id": 0,
            "name": "$_id",
            "moyenne": 1
        }
    }
])

# Récupère le résultat sous forme de liste et l'affiche joliment
result = list(cursor)
pprint.pprint(result)

cursor = db.restaurants.find({"name": "Balcon Quiteno Restaurant"})
l_resultat2_1 = list(cursor)
drc_resultat2_1=l_resultat2_1[0]

pprint.pprint(drc_resultat2_1)

cursor = db.restaurants.aggregate([{"$match":{"name": "Balcon Quiteno Restaurant"}},
                                   
                                   {"$project":{"grades.date":1,
                                                "grades.score":1,
                                                "_id":0
                                                }
                                    },
                                    ])
l_resultat3 = list(cursor)
drc_resultat3=l_resultat3[0]


dates_list = [grade.get("date") for grade in drc_resultat3.get("grades", [])]
scores_list = [grade.get("score") for grade in drc_resultat3.get("grades", [])]


plt.plot(dates_list,scores_list)
plt.show()

print()
print("Clessement des meilleurs style de cuisine de Manhattan")
print()


cursor = db.restaurants.aggregate([
    {
        "$match": {"borough": "Manhattan"}
    },
    {
        "$unwind": "$grades"
    },
    {
        "$group": {
            "_id": "$cuisine",
            "moyenne": {"$avg": "$grades.score"}
        }
    },
    {
        "$sort": {"moyenne": -1}
    },
    {
         "$limit":5
     },
    {
        "$project": {
            "_id": 0,
            "cuisine": "$_id",
            "moyenne": 1
        }
    }
])

# Récupère le résultat sous forme de liste et l'affiche joliment
result = list(cursor)
pprint.pprint(result)

print()
print("Clessement des meilleurs style de cuisine de NYC")
print()


cursor = db.restaurants.aggregate([

    {
        "$unwind": "$grades"
    },
    {
        "$group": {
            "_id": "$cuisine",
            "moyenne": {"$avg": "$grades.score"}
        }
    },
    {
        "$sort": {"moyenne": -1}
    },
    {
         "$limit": 5
     },
    {
        "$project": {
            "_id": 0,
            "cuisine": "$_id",
            "moyenne": 1
        }
    }
])

# Récupère le résultat sous forme de liste et l'affiche joliment
result = list(cursor)
pprint.pprint(result)

print()
print("Clessement des meilleurs style de cuisine de la rue")
print()


cursor = db.restaurants.aggregate([
    {
        "$match": {"address.street": "Sherman Avenue"}
    },
    {
        "$unwind": "$grades"
    },
    {
        "$group": {
            "_id": "$cuisine",
            "moyenne": {"$avg": "$grades.score"}
        }
    },
    {
        "$sort": {"moyenne": -1}
    },
    {
         "$limit":5
     },
    {
        "$project": {
            "_id": 0,
            "cuisine": "$_id",
            "moyenne": 1
        }
    }
])

# Récupère le résultat sous forme de liste et l'affiche joliment
result = list(cursor)
pprint.pprint(result)

print()
print("Les meilleurs rues pour faire du spanish (avec au moins 2 restaurants)")
print()



# Agrégation pour trouver la meilleure rue pour les restaurants espagnols dans le quartier "Manhattan" avec au moins 2 restaurants dans la rue
cursor = db.restaurants.aggregate([
    {
        "$match": {"cuisine": "Spanish", "borough": "Manhattan"}
    },
    {
        "$unwind": "$grades"
    },
    {
        "$group": {
            "_id": {"street": "$address.street", "name": "$name"},
            "moyenne": {"$avg": "$grades.score"}
        }
    },
    {
        "$group": {
            "_id": "$_id.street",
            "count": {"$sum": 1},
            "moyenne_totale": {"$avg": "$moyenne"}
        }
    },
    {
        "$match": {"count": {"$gte": 2}}
    },
    {
        "$sort": {"moyenne_totale": -1}
    },
    {
        "$project": {
            "_id": 0,
            "street": "$_id",
            "moyenne_totale": 1
        }
    }
])

# Récupère le résultat sous forme de liste et l'affiche joliment
result = list(cursor)
pprint.pprint(result)


print()
print("Les restaurants de cette rue:")



cursor = db.restaurants.aggregate([
    {
        "$match": {
            "cuisine": "Spanish",
            "address.street": "Nagle Avenue"
        }
    },
    {
        "$unwind": "$grades"
    },
    {
        "$group": {
            "_id": "$name",
            "moyenne": {"$avg": "$grades.score"}
        }
    },
    {
        "$sort": {"moyenne": -1}
     },
    {
        "$project": {
            "_id": 0,
            "name": "$_id",
            "moyenne": 1
        }
    }
])

# Récupère le résultat sous forme de liste et l'affiche joliment
result = list(cursor)
pprint.pprint(result)

print()
print("Les différents restaurants de la rue")



# Agrégation pour obtenir les scores des restaurants espagnols de la rue "Sherman Avenue" en fonction de la date
cursor = db.restaurants.aggregate([
    {
        "$match": {
            "cuisine": "Spanish",
            "address.street": "Sherman Avenue",
            "grades.date": {"$exists": True}
        }
    },
    {
        "$unwind": "$grades"
    },
    {
        "$match": {
            "grades.date": {"$exists": True}
        }
    },
    {
        "$project": {
            "name": 1,
            "date": "$grades.date",
            "score": "$grades.score"
        }
    },
    {
        "$sort": {"date": 1}
    }
])

# Récupère le résultat sous forme de liste
result = list(cursor)

# Prépare les données pour le graphique
restaurant_names = set([restaurant["name"] for restaurant in result])
date_scores = {name: {"dates": [], "scores": []} for name in restaurant_names}

for restaurant in result:
    name = restaurant["name"]
    date = restaurant["date"]
    score = restaurant["score"]
    date_scores[name]["dates"].append(date)
    date_scores[name]["scores"].append(score)

# Crée le graphique
plt.figure(figsize=(10, 6))
for name, data in date_scores.items():
    plt.plot(data["dates"], data["scores"], label=name)

plt.title("Scores des restaurants espagnols de la rue Sherman Avenue en fonction de la date")
plt.xlabel("Date")
plt.ylabel("Score")
plt.legend()
plt.show()


cursor = db.restaurants.find({"name": "Fantastic Restaurant & Lounge"})
l_resultat2_1 = list(cursor)
drc_resultat2_1=l_resultat2_1[0]


pprint.pprint(drc_resultat2_1)



# Nombre total de restaurants dans la ville
total_restaurants = db.restaurants.count_documents({})

print("Nombre total de restaurants dans la ville :", total_restaurants)

