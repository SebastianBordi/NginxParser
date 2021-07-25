#!/usr/bin/python3
import sys, getopt
import mysql.connector
import os.path, os
import data
import json

helpMessage: str = "Nginx log parser. main.py -f <nginx log file>  -s <conection string> \n - valid string con: \"Server=myServerAddress;Port=1234;Database=myDataBase;Uid=myUsername;Pwd=myPassword;\"\n\n\t\t-f , --log-file   \t Nginx log file \n" + "\t\t-s , --con-string \t Conection string to the database "
path: str
conString: str

def main (argv: list[str]):
    global path
    global conString
    parseArguments(argv)
    if testMySqlConnection() == False :
        print("error connecting to de database")
        os.exit(-2)
    if os.path.isfile(path) == False:
        print("file does not exist")
        os.exit(-2)
    
    processFile()
    


def parseArguments(argv: any):
    global path
    global conString 

    try:
        opts, arg = getopt.getopt(argv, "hf:s:", ["log-file=", "con-string="])
    except getopt.GetoptError:
        print ("error getting the arguments, execution format: python3 main.py -f <nginx log file>  -s <conection string>")
        sys.exit(-2)
    for opt, arg in opts:
        if opt == '-h':
            print (helpMessage)
            sys.exit(0)
        elif opt in ("-f", "--log-file"):
            path = arg
        elif opt in ("-s", "--con-string"):
            conString = arg

def testMySqlConnection():
    try:
        cnx = mysql.connector.connect(
            user=getParamFromConnectionString("Uid"),
            password=getParamFromConnectionString("Pwd"),
            host=getParamFromConnectionString("Server"),
            port=getParamFromConnectionString("Port"),
            database=getParamFromConnectionString("Database"))

        query = "SELECT 10"
        crs = cnx.cursor()
        cnx.commit()
        cnx.close()
        return True
    except mysql.connector.Error as err:
        print(err)
        cnx.close()
        return False
        
def getParamFromConnectionString(param: str):
    splited = conString.split(";", -1)
    for value in splited:
        if value.startswith(param):
            return value.split("=", -1)[1]

    return ""

def processFile():
    global path
    processedLines = 0
    unprocessedLines = 0
    file = open(path)
    parsedData = data.ParsedLog()
    while True:
        line = file.readline()
        line = line.replace("\n", "")
        if not line:
            break

        splited = line.split(" ", -1 )
        if len(splited) < 11:
            print("this line cant be proccessed ")
            print("\t\t" + line)
            unprocessedLines += 1
            continue

          
        # get origin IP
        parsedData.ip = splited[0]
        
        #get date
        parsedData.date = splited[3][1:]

        #get http method
        parsedData.method = splited[5][1:]

        #get response code
        try:
            parsedData.responseCode = int(splited[8])
        except ValueError:
            print("error parsing the response code")
            print(line)
            break;

        #get url 
        parsedData.url = splited[10]

        #get endpoint 
        parsedData.endpoint = splited[6]         
        
        parsedData.browserData = line.split(" \"", -1)[3]

        parsedData.original = line
        processedLines += 1
        saveParsedData(parsedData)
    
    print("Processed Line: " + str(processedLines))
    print("Unprocessed Line: " + str(unprocessedLines))


def saveParsedData(parsedData: data.ParsedLog):
    query = "INSERT INTO logs.logs (ip, `datetime`, `method`, response_code, url, endpoint, browser_data, original) "
    query += str.format("VALUES('{0}', '{1}', '{2}', {3}, '{4}', '{5}', '{6}', '{7}');", parsedData.ip, parsedData.date, parsedData.method, parsedData.responseCode, parsedData.url, parsedData.endpoint, parsedData.browserData, parsedData.original)
    print(query)
    try:
        connection = getCon()
        if (connection):
            cursor =  connection.cursor()
            cursor.execute(query)
            connection.commit()
            connection.close()
        else:
            print("Connection error")
            os.exit(-2)
    except err:
        print("error saving the data")
        print(err)

def getCon():
    globals()
    cnx: any
    try:
        cnx = mysql.connector.connect(user=getParamFromConnectionString("Uid"),
                    password=getParamFromConnectionString("Pwd"),
                    host=getParamFromConnectionString("Server"),
                    port=getParamFromConnectionString("Port"),
                    database=getParamFromConnectionString("Database"))

    except mysql.connector.Error as err:
        print(err)

    return cnx
            

if __name__ == "__main__":
    main(sys.argv[1:])
