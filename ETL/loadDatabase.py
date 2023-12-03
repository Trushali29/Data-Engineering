import mysql.connector
import csv

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="root",
  database = "cars_dataset"
)

mycursor = mydb.cursor()

with open('dealership_transformed_data.csv', mode = 'r') as csvfile:
    csv_reader = csv.reader(csvfile)
    header = next(csv_reader)
    for row in csv_reader:
        sql = "INSERT INTO cars (car_model,year_of_manufacture,price,fuel) VALUES (%s,%s,%s,%s)"
        mycursor.execute(sql,tuple(row))
        print("Record inserted")



sql2 = "SELECT * FROM cars"
mycursor.execute(sql2)
myresult = mycursor.fetchall()
for x in myresult:
    print(x)

    
mydb.commit()
mycursor.close()
