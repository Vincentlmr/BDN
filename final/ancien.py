#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 20 13:44:29 2023

@author: vlemeur
"""
--------------------------------------------------------------------------------
MongoDB
--------------------------------------------------------------------------------
Question1:

db.restaurants.aggregate([
    {
        "$project": {
            "_id": 0,
            "address.zipcode": 1,
            "name":1
        }
    }
])

--------------------------------------------------------------------------------

Question2:

db.restaurants.aggregate([
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

--------------------------------------------------------------------------------

Question3:

db.restaurants.aggregate([
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

--------------------------------------------------------------------------------

Question4:


db.restaurants.aggregate([

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

--------------------------------------------------------------------------------
Question 5:

db.restaurants.aggregate([
    {
        "$unwind": "$grades"
    },
    {
        "$match": {"grades.score": {"$lt": 20}}
    },
    {
        "$group": {
            "_id": "$cuisine",
            "total_grade": {"$sum": 1}
        }
    }
])

--------------------------------------------------------------------------------
Question 6:
db.restaurants.aggregate([
    {
        "$unwind": "$grades"
    },
    
    {
     "$match":{"restaurant_id": "40356649"}
     },
    {
        "$sort": {"grades.score": -1}
    }
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

--------------------------------------------------------------------------------
ORIENDB
--------------------------------------------------------------------------------
Question 1:

MATCH {Class:Profiles, where: (Name='Fanny')}-HasStayed->{ Class:Profiles, as:profile},{Class:Hotels, where: (Name='Ragno')}-HasStayed->{Class:Hotels, as:profile} return profile.Surname