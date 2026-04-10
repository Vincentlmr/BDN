#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 20 11:42:54 2023

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
     "$unwind": "$grades"
     },
    {
        "$project": {
            "_id": 0,
            "cuisine": 1,
            "grades.score": 1,
            "name":1
        }
    }
])

result = list(cursor)
print(result)

print()
print("Récupérez la liste des quartiers où il y a au moins un restaurant qui a au moins 4 scores et où tous ses scores sont inférieurs à 5.")
    
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
            "max_score": {"$lt": 5},
            "min_score": {"$lt": 5}
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
print("Calculez pour le quartier du Bronx, le nombre de restaurants par nombre de grades.")

cursor = db.restaurants.aggregate([
    {
        "$match": {"borough": "Bronx"}
    },
    {
        "$project": {
            "restaurant_id": 1,
            "num_grades": {"$size": "$grades"}
        }
    },
    {
        "$group": {
            "_id": "$num_grades",
            "restaurant_count": {"$sum": 1}
        }
    },
    {
        "$sort": {"_id": 1}
    }
])

result = list(cursor)

for entry in result:
    print(f"{entry['restaurant_count']} restaurants ayant {entry['_id']} scores")

print()
print("Calculez la moyenne par quartier des scores pour chaque restaurant et classer les résultats par ordre croissant des moyennes")

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
            "avg_score": {"$avg": "$grades.score"}
        }
    },
    {
        "$group": {
            "_id": "$_id.borough",
            "average_score": {"$avg": "$avg_score"}
        }
    },
    {
        "$sort": {"average_score": 1}
    }
])

result = list(cursor)

for entry in result:
    print(f"Dans le quartier {entry['_id']} le score moyen des restaurants est de {entry['average_score']}")

print()
print("Calculez le nombre total de grades de grade B par type de cuisine")

cursor = db.restaurants.aggregate([
    {
        "$unwind": "$grades"
    },
    {
        "$match": {"grades.grade": "B"}
    },
    {
        "$group": {
            "_id": "$cuisine",
            "total_grade_B": {"$sum": 1}
        }
    }
])

result = list(cursor)

for entry in result:
    print(f"Type de cuisine : {entry['_id']}, Nombre total de grades de grade B : {entry['total_grade_B']}")

print()
print("Calculez Le nombre moyen de grades de grade A par quartier pour les restaurants de type Pizza")

cursor = db.restaurants.aggregate([
    {
        "$match": {"cuisine": "Pizza"}
    },
    {
        "$unwind": "$grades"
    },
    {
        "$match": {"grades.grade": "A"}
    },
    {
        "$group": {
            "_id": "$borough",
            "average_grade_A": {"$avg": 1}
        }
    }
])

result = list(cursor)

for entry in result:
    print(f"Quartier : {entry['_id']}, Nombre moyen de grades de grade A pour les restaurants de type Pizza : {entry['average_grade_A']}")
