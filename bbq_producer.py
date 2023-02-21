""" Creating a producer, project for Streaming Data
    Author: Missy Bernskoetter
    Date: 2/14/2023 
    I am assuming this will have multiple consumers, maybe one for each
    column. smoker-temps.csv has 4 columns:

    [0] Time = Date-time stamp for the sensor reading
    [1] Channel1 = Smoker Temp --> send to message queue "01-smoker"
    [2] Channe2 = Food A Temp --> send to message queue "02-food-A"
    [3] Channe3 = Food B Temp --> send to message queue "02-food-B" 
    This will mean 3 message queues and each one will have a date-time 
    stamp included. """
# modules needed for code, input what you need
import pika
import sys
import webbrowser
import csv
import time
#pulls up RabbitMQ admin page if offer is true
def offer_rabbitmq_admin_site(show_offer):
    """Offer to open the RabbitMQ Admin website"""
    if show_offer == "True":
        ans = input("Would you like to monitor RabbitMQ queues? y or n ")
        print()
        if ans.lower() == "y":
            webbrowser.open_new("http://localhost:15672/#/queues")
            print()

def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue, one for each channel
        message (str): the message to be sent to the queue
    """
try:
    # create a blocking connection to the RabbitMQ server
    conn = pika.BlockingConnection(pika.ConnectionParameters(host))
    # use the connection to create a communication channel
    ch = conn.channel()
    #the three queues we will use for the producing.
    ch.queue_declare(queue=queue_name, durable=True)
    # Use the channel to publish a message to the queue
    # every message passes through an exchange
    ch.basic_publish(exchange="", routing_key=queue_name, body= message)
    # print a message to the console for the user
    print(f" [x] Sent {message}")
except pika.exceptions.AMQPConnectionError as e:
     print(f"Error: Connection to RabbitMQ server failed: {e}")
     sys.exit(1)
finally:
     # close connection to the server
     conn.close()
# Declare a host name
host="localhost"
# Set Queueclear to true to clear the queue
Queueclear=True
def offer_Queueclear(Queueclear):
     """offer to open RabbitMQ Admin website"""
if Queueclear==True:
        ans = input("would you like to clear the queues? y or n")
        print()
        if ans.lower() == "y":
             #create a blocking connection to the RabbitMQ server
             conn = pika.BlockingConnection(pika.ConnectionParameters(host))
             # use the connection to create a communication channel
             ch = conn.channel()
             # delete the queues
             ch.queue_delete("01-smoker")
             ch.queue_delete("02-food-A")
             ch.queue_delete("03-food-B")
             print("Queue cleared!")

offer_Queueclear(Queueclear)
offer_rabbitmq_admin_site(True)
"""Create CSV function to read from file and turn it into a message"""
# opens and reads smoker file
with open("smoker-temps.csv", 'r') as file:
     reader=csv.reader(file, delimiter=",")
#read in rows
for row in reader:
     #read just the timestamp and store it
     fstringtime=f"{row[0]}"
     # read smoker temps and store it
     smokertemp=f"{row[1]}"
     # read food A and store it
     foodA=f"{row[2]}"
     # read food B and store it
     foodB=f"{row[3]}"
# set up messages
smoker_message=f"{fstringtime},{smokertemp}"
foodA_message=f"{fstringtime},{foodA}"
foodB_message=f"{fstringtime},{foodB}"
# get the messages to the consumers
send_message(host,"01-smoker",smoker_message)
send_message(host,"02-food-A",foodA_message)
send_message(host,"03-food-B",foodB_message)
# one message every 30 seconds with sleep time
time.sleep(30)
