#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 20 13:43:57 2023

@author: vlemeur
"""

from pymongo import MongoClient
import pprint
import matplotlib.pyplot as plt
client = MongoClient(host="localhost", port=27017)

db = client["tp"]

print("Récupérez la liste complète des restaurants où vous n’affichez que le score des grades et le type de cuisine")

cursor = db.restaurants.aggregate([
    {
        "$project": {
            "_id": 0,
            "address.zipcode": 1,
            "cuisine":1
        }
    }
])

result = list(cursor)
print(result)

print(" Les quartiers où il y a au moins un restaurant qui a au moins 4 scores et où tous ses scores sont inérieurs à 15")

cursor = db.restaurants.aggregate([
    {
        "$unwind": "$grades"
    },
    {
        "$group": {
            "_id": {
                "borough": "$borough",
                "restaurant_id": "$restaurant_id"
            },
            "count_scores": {"$sum": 1},
            "max_score": {"$max": "$grades.score"},
            "min_score": {"$min": "$grades.score"}
        }
    },
    {
        "$match": {
            "count_scores": {"$gte": 4},
            "max_score": {"$lt": 15},
            "min_score": {"$lt": 15}
        }
    },
    {
        "$group": {
            "_id": "$_id.borough"
        }
    },
    {
        "$project": {
            "_id": 0,
            "borough": "$_id"
        }
    }
])

result = list(cursor)
print(result)


print()
print("Compte le nombre de restaurants pizzas pour chaque quartier")
print()



cursor = db.restaurants.aggregate([
    {
        "$match": {"cuisine": "Pizza"}
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


result = list(cursor)
pprint.pprint(result)

print()
print("Clessement des meilleurs style de cuisine pas moyenne par ordre croissant")
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
        "$sort": {"moyenne": 1}
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

cursor = db.restaurants.aggregate([
    {
        "$unwind": "$grades"
    },
    {
        "$match": {
            "grades.score": {"$lt": 20}
        }
    },
    {
        "$group": {
            "_id": "$cuisine",
            "total_grades_lt_20": {"$sum": 1}
        }
    },
    {
        "$project": {
            "_id": 0,
            "cuisine": "$_id",
            "total_grades_lt_20": 1
        }
    }
])

result = list(cursor)
for item in result:
    print(f"Cuisine: {item['cuisine']}, Nombre total de grades de score inférieur à 20 : {item['total_grades_lt_20']}")

print( " Autre ")

print(" Les quartiers où il y a au moins un restaurant qui a au moins 4 scores et où tous ses scores sont supérieur à 20")

cursor = db.restaurants.aggregate([
    {
        "$unwind": "$grades"
    },
    {
        "$group": {
            "_id": {
                "borough": "$borough",
                "restaurant_id": "$restaurant_id"
            },
            "count_scores": {"$sum": 1},
            "min_score": {"$min": "$grades.score"}
        }
    },
    {
        "$match": {
            "count_scores": {"$gte": 4},
            "min_score": {"$gt": 15}
        }
    },
    {
        "$group": {
            "_id": "$_id.borough"
        }
    },
    {
        "$project": {
            "_id": 0,
            "borough": "$_id"
        }
    }
])

result = list(cursor)
print(result)

print()
print("Compte le nombre de restaurants par type de cuisine pour le quartier Queens")
print()



cursor = db.restaurants.aggregate([
    {
        "$match": {"borough": "Queens"}
    },
    {
        "$group": {
            "_id": "$cuisine",
            "count": {"$sum": 1}
        }
    },
    {
        "$project": {
            "_id": 0,
            "cuisine": "$_id",
            "count": 1
        }
    },
    {
        "$sort": {"count": 1}
    }
])

result = list(cursor)
for item in result:
    print(f"Cuisine: {item['cuisine']}, Nombre de restaurants: {item['count']}")
    

print()
print("Compte le score moyen par quartier par odre croissant")
print()
    
cursor = db.restaurants.aggregate([
    {
        "$unwind": "$grades"
    },
    {
        "$group": {
            "_id": "$borough",
            "avg_score": {"$avg": "$grades.score"}
        }
    },
    {
        "$project": {
            "_id": 0,
            "borough": "$_id",
            "avg_score": 1
        }
    },
    {
        "$sort": {"avg_score": 1}
    }
])

result = list(cursor)
for item in result:
    print(f"Quartier: {item['borough']}, Score moyen: {item['avg_score']}")

cursor = db.restaurants.aggregate([
    {
        "$unwind": "$grades"
    },
    {
        "$match": {
            "grades.score": {"$gt": 20}
        }
    },
    {
        "$group": {
            "_id": "$borough",
            "total_grades_gt_20": {"$sum": 1}
        }
    },
    {
        "$project": {
            "_id": 0,
            "borough": "$_id",
            "total_grades_gt_20": 1
        }
    }
])

result = list(cursor)
for item in result:
    print(f"Quartier: {item['borough']}, Nombre total de grades de score supérieur à 20 : {item['total_grades_gt_20']}")

print()
print("le restaurant sont les grades sont classée par scores décroissants")
print()

cursor = db.restaurants.aggregate([
    {
        "$match": {
            "restaurant_id": "40356151"
        }
    },
    {
        "$unwind": "$grades"
    },
    {
        "$sort": {"grades.score": -1}
    },
    {
        "$group": {
            "_id": "$_id",
            "restaurant": {"$first": "$$ROOT"}
        }
    },
    {
        "$project": {
            "_id": 0,
            "restaurant": 1
        }
    }
])

result = list(cursor)
print(result[0]["restaurant"])