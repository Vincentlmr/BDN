import pprint
import datetime

from pymongo import MongoClient
client = MongoClient(host="localhost", port=27017)
db = client["tp"]

class bcolors:
    OK = '\033[92m'
    FAIL = '\033[93m'
    ENDC = '\033[0m'

def test(which,te,val):
    assert te == val, bcolors.FAIL + "Test "+which+" Fail" + bcolors.ENDC + "\n" + str(te) + "\n est différent de \n" + str(val)

# TP 3Bis
#
# Complétez uniquement les # A Toi de jouer
# Vous avez 10 questions à completez.
# Une fois que vous avez répondu à une question, testez votre code en faisant
# $ python source_tp3bis.py
# Si un 'OK' apparait, c'est bon signe :) dans le cas contraire (un "fail") recommencez.
# Le type de questions de ce TP, correspondra au type de questions MongoDB que vous aurez au TP final.


# Question 1 : Ajouter une requête qui affiche uniquement le "name" et la "cuisine" d'un restaurant dont on vous donne l'"id"
def get_reponse_1(id):
    cursor = db.restaurants.aggregate([
        {
            "$match" : {
                "restaurant_id" : id
            }},
        {
            "$project" : {
                "_id":0,
                "name":1,
                "cuisine" : 1
                }
        
            }
            
        
    ])
    return list(cursor)

test("1.1",get_reponse_1('41601082'),[{'cuisine': 'American ', 'name': 'Danny Boys Tavern'}])
test("1.2",get_reponse_1('41600331'),[{'cuisine': 'Irish', 'name': "Connolly'S"}])
test("1.3",get_reponse_1('41600577'),[{'cuisine': 'Asian', 'name': 'Wild Ginger'}])

print("Reponse 1 : " + bcolors.OK +"OK" + bcolors.ENDC)


# Question 2 : La même requête que la question 1 en changeant la variable "name" en "nom" (voir solution TP2, question II.21, ligne 21)
def get_reponse_2(id):
    cursor = db.restaurants.aggregate([
        {
            "$match" : {
                "restaurant_id" : id
            }},
        {
            "$project" : {
                "_id":0,
                "nom": "$name",
                "cuisine" : 1
                }
        
            }
    ])
    return list(cursor)

test("2.1",get_reponse_2('40394954'),[{'cuisine': 'Italian', 'nom': 'Campagnola Restaurant'}])
test("2.2",get_reponse_2('40810032'),[{'cuisine': 'Indian', 'nom': 'Al-Mehran Restaurant'}])
test("2.3",get_reponse_2('41638986'),[{'cuisine': 'Sandwiches/Salads/Mixed Buffet', 'nom': 'Crave Sandwiches'}])

print("Reponse 2 : " + bcolors.OK +"OK" + bcolors.ENDC)


# Question 3 : La même requête que la question 2 en ajoutant en plus une variable "nbgrades" qui va stocker la taille de la liste de la variable "grades" (voir solution TP2, question II.16b, ligne 5)
def get_reponse_3(id):
    cursor = db.restaurants.aggregate([
        {
            "$match" : {
                "restaurant_id" : id
            }},
        {
            "$project" : {
                "_id":0,
                "nom": "$name",
                "cuisine" : 1,
                "nbgrades" : {"$size" : "$grades"},
                }
        
            }
    ])
    return list(cursor)


test("3.1",get_reponse_3('41527622'),[{'cuisine': 'Bakery', 'nbgrades': 5, 'nom': 'New York Cake Bakery'}])
test("3.2",get_reponse_3('41467391'),[{'cuisine': 'Peruvian', 'nbgrades': 3, 'nom': 'La Brasa Peruana'}])
test("3.3",get_reponse_3('41359592'),[{'cuisine': 'Thai', 'nbgrades': 4, 'nom': 'Thailand Restaurant'}])


print("Reponse 3 : " + bcolors.OK +"OK" + bcolors.ENDC)


# Question 4 : Ajouter une requête qui affiche uniquement le "score" et le "grade" de tous les "grades" d'un restaurant dont on vous donne l'"id" (voir solution TP2, question II.18, ligne 10 et question II.21, ligne 21)
def get_reponse_4(id):
    cursor = db.restaurants.aggregate([
        {
            "$match" : {
                "restaurant_id" : id
            }},
        {
            "$unwind": "$grades"
        },
        {
            "$project": {
                "_id": 0,
                "score": "$grades.score",
                "grade": "$grades.grade"
            }
        }
    ])
    return list(cursor)

test("4.1",get_reponse_4('41431253'),[{'score': 20, 'grade': 'B'}, {'score': 27, 'grade': 'B'}, {'score': 13, 'grade': 'A'}])
test("4.2",get_reponse_4('40391284'),[{'score': 9, 'grade': 'A'}, {'score': 10, 'grade': 'A'}, {'score': 9, 'grade': 'A'}, {'score': 12, 'grade': 'A'}, {'score': 13, 'grade': 'A'}, {'score': 7, 'grade': 'A'}])
test("4.3",get_reponse_4('41658324'),[{'score': 15, 'grade': 'B'}, {'score': 28, 'grade': 'C'}, {'score': 29, 'grade': 'C'}, {'score': 13, 'grade': 'A'}, {'score': 10, 'grade': 'A'}, {'score': 9, 'grade': 'P'}])


print("Reponse 4 : " + bcolors.OK +"OK" + bcolors.ENDC)

# Question 5 : La même requête que la question 4 en regroupant par "grade" (classée de manière croissante) la liste des scores avec les valeurs dupliquées (dans une variable "scores") de manière croissante (voir https://www.mongodb.com/docs/v4.2/reference/operator/aggregation/push/ en triant de manière croissante les scores avant de grouper et ensuite en triant sur les grades)
def get_reponse_5(id):
    cursor = db.restaurants.aggregate([
        {
            "$match" : {
                "restaurant_id" : id
            }
        },
        {
            "$unwind": "$grades"
        },
        {   
            "$sort": {"grades.grade": 1, "grades.score": 1}
        },
        {
            "$group": {
                "_id": "$grades.grade",
                "scores": {"$push": "$grades.score"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "grade": "$_id",
                "scores": 1
            }
        },
        {   "$sort": {"grade": 1}
         },
    ])
    return list(cursor)

test("5.1",get_reponse_5('41431253'), [{'scores': [13], 'grade': 'A'}, {'scores': [20, 27], 'grade': 'B'}])
test("5.2",get_reponse_5('40391284'), [{'scores': [7, 9, 9, 10, 12, 13], 'grade': 'A'}])
test("5.3",get_reponse_5('41658324'), [{'scores': [10, 13], 'grade': 'A'}, {'scores': [15], 'grade': 'B'}, {'scores': [28, 29], 'grade': 'C'}, {'scores': [9], 'grade': 'P'}])

print("Reponse 5 : " + bcolors.OK +"OK" + bcolors.ENDC)




# Question 6 : Ajouter une requête qui affiche uniquement le "name" et les "coord" dans une variable "x" et une variable "y" d'un restaurant dont on vous donne l'"id" (voir https://www.mongodb.com/docs/v4.2/reference/operator/aggregation/arrayElemAt/)
def get_reponse_6(id):
    cursor = db.restaurants.aggregate([
        {
            "$match" : {
                "restaurant_id" : id
            }
        },
        {
            "$project": {
                "_id": 0,
                "name": 1,
                "x" : {
                    "$arrayElemAt" : [ "$address.coord", 0],
                },
                "y" : {
                    "$arrayElemAt" : [ "$address.coord", 1],
                },
                
            }
        }
    ])
    return list(cursor)

test("6.1",get_reponse_6('41431253'), [{'name': "King'S Bar Cafe(Class)", 'x': -74.0056029, 'y': 40.633354}])
test("6.2",get_reponse_6('40391284'), [{'name': 'Bar Association Of The City Of Ny Kitchen', 'x': -73.9821691, 'y': 40.7554761}])
test("6.3",get_reponse_6('41658324'), [{'name': 'Mi Casa Restaurant', 'x': -73.83260899999999, 'y': 40.699166}]
)


print("Reponse 6 : " + bcolors.OK +"OK" + bcolors.ENDC)

# Question 7 : Ajouter une requête qui affiche, pour un quartier "bo", le nombre de grades  qui sont le lundi, le mardi et ainsi de suite jusqu'au dimanche. La sortie doit donc être une liste de documents avec une seule variable "nombre" (voir https://www.mongodb.com/docs/v4.2/reference/operator/aggregation/isoDayOfWeek/)
def get_reponse_7(bo):
    cursor = db.restaurants.aggregate([
        {
            "$match" : {
                "borough" : bo
            }
        },
        {
            "$unwind": "$grades"
        },
        {
            "$project": {
                "_id": 0,
                "week": {
                    "$isoDayOfWeek": "$grades.date"
                }                
            }
        },
        {
            "$group" : {
                    "_id":"$week",
                    "nombre" : {
                        "$sum" : 1
                    }
                }
        },
        {
            "$sort" : { "_id":1}   
        },
        {
            "$project": {
                    "_id":0
                }
        }
    ])
    return list(cursor)

test("7.1",get_reponse_7('Brooklyn'), [{'nombre': 3035}, {'nombre': 4693}, {'nombre': 5279}, {'nombre': 5348}, {'nombre': 2636}, {'nombre': 953}, {'nombre': 19}])
test("7.2",get_reponse_7('Manhattan'), [{'nombre': 7719}, {'nombre': 8360}, {'nombre': 7818}, {'nombre': 7414}, {'nombre': 6633}, {'nombre': 669}, {'nombre': 9}])
test("7.3",get_reponse_7('Queens'), [{'nombre': 2866}, {'nombre': 4242}, {'nombre': 5134}, {'nombre': 5179}, {'nombre': 2475}, {'nombre': 968}, {'nombre': 13}])


print("Reponse 7 : " + bcolors.OK +"OK" + bcolors.ENDC)



# # Question 8 : Ajouter une requête qui affiche uniquement les différents "grade" (dans l'ordre croissant) et le "nombre" de grades de l'ensemble des restaurants qui sont de type de cuisines "cui"
# def get_reponse_8(cui):
#     cursor = db.restaurants.aggregate([
#         # A Toi de jouer
#     ])
#     return list(cursor)

# test("8.1",get_reponse_8('Greek'),[{'nombre': 385, 'grade': 'A'}, {'nombre': 49, 'grade': 'B'}, {'nombre': 6, 'grade': 'C'}, {'nombre': 3, 'grade': 'P'}, {'nombre': 6, 'grade': 'Z'}])
# test("8.2",get_reponse_8('Asian'),[{'nombre': 754, 'grade': 'A'}, {'nombre': 239, 'grade': 'B'}, {'nombre': 55, 'grade': 'C'}, {'nombre': 5, 'grade': 'Not Yet Graded'}, {'nombre': 22, 'grade': 'P'}, {'nombre': 27, 'grade': 'Z'}])
# test("8.3",get_reponse_8('French'),[{'nombre': 1087, 'grade': 'A'}, {'nombre': 156, 'grade': 'B'}, {'nombre': 33, 'grade': 'C'}, {'nombre': 6, 'grade': 'Not Yet Graded'}, {'nombre': 13, 'grade': 'P'}, {'nombre': 24, 'grade': 'Z'}])


# print("Reponse 8 : " + bcolors.OK +"OK" + bcolors.ENDC)

# # Question 9 : Ajouter une requête qui affiche un restaurant dont on vous donne l'"id" de tel sorte que les grades soit classée par scores croissants.
# # Conseil : Isoler les grades pour les trier par score et regrouper avec tous les autres variables
# def get_reponse_9(id):
#     cursor = db.restaurants.aggregate([
#         # A Toi de jouer
#     ])
#     return list(cursor)


# test("9.1",get_reponse_9('41431253'), [{'address': {'building': '5918', 'coord': [-74.0056029, 40.633354], 'street': 'Fort Hamilton Parkway', 'zipcode': '11219'}, 'borough': 'Brooklyn', 'cuisine': 'Asian', 'grades': [{'date': datetime.datetime(2013, 1, 10, 0, 0), 'grade': 'A', 'score': 13}, {'date': datetime.datetime(2014, 4, 23, 0, 0), 'grade': 'B', 'score': 20}, {'date': datetime.datetime(2013, 7, 10, 0, 0), 'grade': 'B', 'score': 27}], 'name': "King'S Bar Cafe(Class)", 'restaurant_id': '41431253'}])
# test("9.2",get_reponse_9('40391284'), [{'address': {'building': '42', 'coord': [-73.9821691, 40.7554761], 'street': 'West 44 Street', 'zipcode': '10036'}, 'borough': 'Manhattan', 'cuisine': 'American ', 'grades': [{'date': datetime.datetime(2011, 5, 23, 0, 0), 'grade': 'A', 'score': 7}, {'date': datetime.datetime(2014, 5, 9, 0, 0), 'grade': 'A', 'score': 9}, {'date': datetime.datetime(2012, 10, 17, 0, 0), 'grade': 'A', 'score': 9}, {'date': datetime.datetime(2013, 4, 3, 0, 0), 'grade': 'A', 'score': 10}, {'date': datetime.datetime(2012, 4, 19, 0, 0), 'grade': 'A', 'score': 12}, {'date': datetime.datetime(2011, 10, 25, 0, 0), 'grade': 'A', 'score': 13}], 'name': 'Bar Association Of The City Of Ny Kitchen', 'restaurant_id': '40391284'}])
# test("9.3",get_reponse_9('41658324'), [{'address': {'building': '116-20', 'coord': [-73.83260899999999, 40.699166], 'street': 'Jamaica Avenue', 'zipcode': '11418'}, 'borough': 'Queens', 'cuisine': 'Spanish', 'grades': [{'date': datetime.datetime(2012, 5, 4, 0, 0), 'grade': 'P', 'score': 9}, {'date': datetime.datetime(2012, 5, 14, 0, 0), 'grade': 'A', 'score': 10}, {'date': datetime.datetime(2012, 12, 7, 0, 0), 'grade': 'A', 'score': 13}, {'date': datetime.datetime(2014, 12, 3, 0, 0), 'grade': 'B', 'score': 15}, {'date': datetime.datetime(2014, 7, 7, 0, 0), 'grade': 'C', 'score': 28}, {'date': datetime.datetime(2014, 1, 14, 0, 0), 'grade': 'C', 'score': 29}], 'name': 'Mi Casa Restaurant', 'restaurant_id': '41658324'}])

# print("Reponse 9 : " + bcolors.OK +"OK" + bcolors.ENDC)


# # Question 10 : Ajouter une requête qui, pour un "address.zipcode" dont on vous donne le "code", affiche par "grades.grade" (trier par ordre croissant), la liste "cuisinesByScore" qui correspond aux type de cuisine (parmi "Spanish", "Pizza", "American" et "Chinese") avec leur score moyen (le score moyen est alors le score moyen par cuisine et par grades.grade) triée par score croissant.
# # Conseil : Une fois que vous aurez isolé les grades, vous pourrez grouper sur les grades.grade et les cuisines en prenant le score moyen ("$avg"). Ensuite vous pourrez grouper sur les grades.grade en utilisant "$addToSet" pour créer une liste de cuisines/ score moyen.
# def get_reponse_10(code):
#     cursor = db.restaurants.aggregate([
#         # A Toi de jouer
#     ])
#     return list(cursor)


# test("10.1",get_reponse_10('11372'),[{'grade': 'A', 'cuisinesByScore': [{'cuisine': 'Chinese', 'score': 9.383333333333333}, {'cuisine': 'Pizza', 'score': 9.7}, {'cuisine': 'Spanish', 'score': 9.777777777777779}]}, {'grade': 'B', 'cuisinesByScore': [{'cuisine': 'Spanish', 'score': 19.458333333333332}, {'cuisine': 'Chinese', 'score': 20.916666666666668}, {'cuisine': 'Pizza', 'score': 21.333333333333332}]}, {'grade': 'C', 'cuisinesByScore': [{'cuisine': 'Spanish', 'score': 30.875}]}, {'grade': 'Not Yet Graded', 'cuisinesByScore': [{'cuisine': 'Spanish', 'score': 13.5}]}, {'grade': 'P', 'cuisinesByScore': [{'cuisine': 'Spanish', 'score': 3.0}, {'cuisine': 'Chinese', 'score': 8.5}]}, {'grade': 'Z', 'cuisinesByScore': [{'cuisine': 'Pizza', 'score': 21.0}, {'cuisine': 'Spanish', 'score': 22.833333333333332}]}])
# test("10.2",get_reponse_10('10022'),[{'grade': 'A', 'cuisinesByScore': [{'cuisine': 'Pizza', 'score': 8.35483870967742}, {'cuisine': 'Chinese', 'score': 10.393939393939394}, {'cuisine': 'Spanish', 'score': 11.0}]}, {'grade': 'B', 'cuisinesByScore': [{'cuisine': 'Chinese', 'score': 20.272727272727273}, {'cuisine': 'Pizza', 'score': 21.0}]}, {'grade': 'C', 'cuisinesByScore': [{'cuisine': 'Pizza', 'score': 40.0}, {'cuisine': 'Chinese', 'score': 45.0}]}, {'grade': 'P', 'cuisinesByScore': [{'cuisine': 'Chinese', 'score': 2.0}]}, {'grade': 'Z', 'cuisinesByScore': [{'cuisine': 'Chinese', 'score': 25.0}, {'cuisine': 'Pizza', 'score': 26.0}]}])
# test("10.3",get_reponse_10('10128'),[{'grade': 'A', 'cuisinesByScore': [{'cuisine': 'Pizza', 'score': 8.88888888888889}, {'cuisine': 'Spanish', 'score': 10.833333333333334}, {'cuisine': 'Chinese', 'score': 10.89655172413793}]}, {'grade': 'B', 'cuisinesByScore': [{'cuisine': 'Pizza', 'score': 20.1}, {'cuisine': 'Chinese', 'score': 20.75}]}, {'grade': 'C', 'cuisinesByScore': [{'cuisine': 'Chinese', 'score': 52.0}]}, {'grade': 'Not Yet Graded', 'cuisinesByScore': [{'cuisine': 'Chinese', 'score': 28.0}]}, {'grade': 'P', 'cuisinesByScore': [{'cuisine': 'Chinese', 'score': 5.0}]}, {'grade': 'Z', 'cuisinesByScore': [{'cuisine': 'Chinese', 'score': 25.5}]}])


# print("Reponse 10 : " + bcolors.OK +"OK" + bcolors.ENDC)
