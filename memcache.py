import pymongo
from pymemcache.client import base
import time
import ast
databaseServer='35.196.38.94'
databaseServerMongo="mongodb://35.196.38.94:27017/"

def keywordSearch(search,count):# Function to search Keywords and suggest movies to releated to that key word
    client = base.Client((databaseServer, 11211))
    memts=time.time()
    output=(client.get(search.replace(" ","_")+""+str(count)))
    memte=time.time()

    if output is None:
        print("Reading from Database")
        output=[]
        monts=time.time()
        myclient = pymongo.MongoClient(databaseServerMongo)# Mongo Db connection settings
        mydb = myclient["movie"]# Data base connection to Movie
        mycol = mydb["keywords"]#Connection to keyword collection
        metadata = mydb["metadata"]#Connection to metadata Collection
        mydoc = mycol.find({"keywords":{"$regex":search,"$options":"i"}},{"id": 3}).limit(count)#Query to fetch movieId and limiting to 10
        for item in mydoc: # For each movieId fetch from metadata Collection
            result=metadata.find({"id":item["id"]}).limit(count)# Query to fetch Movies based on movieId
            for element in result:# For each elements display Movies
                output.append(element["original_title"])
        monte=time.time()
        print("Mongo Time",monte-monts)
        client.set(search.replace(" ","_")+""+str(count),value=output)
        for element in output:
            print(element)
    else:
        print("Reading from Memcache")
        print("Memcache Time", memte - memts)
        print(output)

def actorSearch(search,count):
    client = base.Client((databaseServer, 11211))
    memts = time.time()
    output = (client.get(search.replace(" ","_")+""+str(count)))
    memte = time.time()

    if output is None:
        print("Reading from Database")
        output = []
        monts = time.time()
        myclient = pymongo.MongoClient(databaseServerMongo)  # Mongo Db connection settings
        mydb = myclient["movie"]
        mycol = mydb["credits"]
        metadata = mydb["metadata"]
        output=[]
        mydoc = mycol.find({"cast": {"$regex": search,"$options":"i"}}, {"id": 3}).limit(count)
        for item in mydoc:
            result = metadata.find({"id": item["id"]}).limit(count)
            for element in result:
                output.append(element["original_title"])
        monte = time.time()
        print("Mongo Time", monte - monts)
        client.set(search.replace(" ","_")+""+str(count), value=output)
        for element in output:
            print(element)
    else:
        print("Reading from Memcache")
        print("Memcache Time", memte - memts)
        print(output)

def avg_length_movie(search,count):
    client = base.Client((databaseServer, 11211))
    memts = time.time()
    output = (client.get("avg_"+search.replace(" ", "_") + "" + str(count)))
    memte = time.time()
    if output is None:
        print("Reading from Database")
        output = []
        monts = time.time()
        myclient = pymongo.MongoClient(databaseServerMongo)  # Mongo Db connection settings
        mydb = myclient["movie"]
        mycol = mydb["metadata"]
        mydoc = mycol.find({"runtime": {"$lte": search}}).limit(count)
        for item in mydoc:
            output.append(item["original_title"])
        monte = time.time()
        print("Mongo Time", monte - monts)
        for element in output:
            print(element)
        client.set("avg_"+search.replace(" ", "_") + "" + str(count), value=output)

    else:
        print("Reading from Memcache")
        print("Memcache Time", memte - memts)
        print(output)

def profitorloss(choice):
    client = base.Client((databaseServer, 11211))
    memts = time.time()

    profit = [{"$project": {"title": 23, "profit": {"$subtract": ["$budget", "$revenue"]}}}]
    loss = [{"$project": {"title": 23, "loss": {"$subtract": ["$revenue", "$budget"]}}}]
    if choice == 0:
        pipline = profit
        code="profit"
    else:
        pipline = loss
        code="loss"
    output = (client.get(code ))
    memte = time.time()
    if output is None:
        print("Reading from Database")
        output = []
        monts = time.time()
        myclient = pymongo.MongoClient(databaseServerMongo)  # Mongo Db connection settings
        mydb = myclient["movie"]
        mycol = mydb["metadata"]

        result = mycol.aggregate(pipline)
        counter=0
        for item in result:
            counter+=1
            if counter==10:
                break
            #print(item)
            if choice==0:
                if item["profit"] is not None and item["profit"] > 0:
                    output.append( str(item["title"])+" "+str(item["profit"])+"$")
            else:
                if item["loss"] is not None and item["loss"]>0:
                    output.append(str(item["title"]+" "+str(item["loss"])+"$"))
        monte = time.time()
        print("Mongo Time", monte - monts)
        for element in output:
            print(element)
        client.set( code, value=output)

    else:
        print("Reading from Memcache")
        print("Memcache Time", memte - memts)
        print(output)

def recomendation(search):
    client = base.Client((databaseServer, 11211))
    memts = time.time()
    output = (client.get(search.replace(" ", "_") ))
    memte = time.time()
    if output is None:
        print("Reading from Database")
        output = []
        monts = time.time()
        myclient = pymongo.MongoClient(databaseServerMongo)  # Mongo Db connection settings
        mydb = myclient["movie"]
        mycol = mydb["metadata"]
        movieName = "Real Steel"
        result = mycol.find({"title": movieName}).limit(1)
        for item in result:
            element = ast.literal_eval(item["genres"])
            stringpipiline = []
            for ele in (element):
                stringpipiline.append(ele["id"])
            newrest = mycol.find({"$and": [{"genres": {"$regex": "" + str(i)}} for i in stringpipiline]}).limit(10)
            for le in newrest:
                if le["title"] != movieName:
                    output.append(le["title"])
        monte = time.time()
        print("Mongo Time", monte - monts)
        client.set(search.replace(" ", "_"), value=output)
        for element in output:
            print(element)
    else:
        print("Reading from Memcache")
        print("Memcache Time", memte - memts)
        print(output)

def displayMenu():
    print("")
    print("1. Quit")
    print("2. Key Word Search")
    print("3. Cast Search")
    print("4. Recommendations")
    print("5. Profit/Loss")
    print("")

if __name__== "__main__":
    option=0
    while option !=1:
        displayMenu()
        option=int(input("Enter Options: "))
        if option==1:
            exit(0)
        elif option==2:
            search = input("Enter Key Word to Search :");  # prompt to input Search key from user
            search = search.lower()
            count = int(input("Enter no of records  to displayed :"))
            keywordSearch(search, count)
        elif option==3:
            search = input("Enter Cast to Search :");  # prompt to input Search key from user
            search = search.lower()
            count = int(input("Enter no of records  to displayed :"))
            actorSearch(search, count)
        elif option==4:
            search = input("Enter Movie Search :");  # prompt to input Search key from user
            search = search.lower()
            recomendation(search)
        elif option==5:
            choice = int(input("Enter Choice 0- Profit 1- Loss :"))
            if choice ==0 or choice == 1:
                print("Valid Entry")
                profitorloss(choice)
            else:
                print("Invalid Entry Please Try again")
        else:
            print("Invalid Entry Please Try again")



