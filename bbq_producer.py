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

import pika
import sys
import webbrowser
import csv
import socket
import time


def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    if show_offer == "True":
        ans = input("Would you like to monitor RabbitMQ queues? y or n ")
        print()
        if ans.lower() == "y":
            webbrowser.open_new("http://localhost:15672/#/queues")
            print()

def send_message(host: str , Channel1: str, Channel2: str, Channel3: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue, one for each channel
        message (str): the message to be sent to the queue
    """
host = 'localhost'
port = 9999
address_tuple = (host, port)

# use an enumerated type to set the address family to (IPV4) for internet
socket_family = socket.AF_INET 

 # use an enumerated type to set the socket type to UDP (datagram)
socket_type = socket.SOCK_DGRAM 

# use the socket constructor to create a socket object we'll call sock
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) 

# read from a file to get data from tasks.csv
input_file = open("smoker-temps.csv", "r")


# create a csv reader for our comma delimited data
reader = csv.reader(input_file, delimiter=",")

# removed the reverse sort due to not being necessary

for row in reader:
   
   # row names as headers, renamed Time(UTC) as Time_UTC_ due to issues
   # with name and coding.
   Time_UTC_, Channel1, Channel2, Channel3 = row


try:
    # create a blocking connection to the RabbitMQ server
    conn = pika.BlockingConnection(pika.ConnectionParameters(host))
    # use the connection to create a communication channel
    ch = conn.channel()
    #the three queues we will use for the producing.
    ch.queue_declare(queue=Channel1, durable=True)
    ch.queue_declare(queue=Channel2, durable=True)
    ch.queue_declare(queue=Channel3, durable=True)
except ValueError:
     pass
    
try:
    Channel1 = round(float(Channel1),1)
    # use an fstring to create a message from our data
    # f before the opening quote 
    smoker_temps = f"[{Time_UTC_}, {Channel1}]"
    # prepare a binary (1s and 0s) message to stream
    MESSAGE = smoker_temps.encode()
    # use the socket sendto() method to send the message
    sock.sendto(MESSAGE, address_tuple)
    ch.basic_publish(exchange="", routing_key= Channel1, body=MESSAGE)
    # print a message to the console for the user
    print(f" [x] Sent Smoker Temp {MESSAGE}")
except ValueError:
        pass
            
try:
    Channel2 = round(float(Channel2),1)
    # use an fstring to create a message from our data
    # f before the opening quote 
    Food_A = f"[{Time_UTC_}, {Channel2}]"
    # prepare a binary (1s and 0s) message to stream
    MESSAGE2 = Food_A.encode()
    # use the socket sendto() method to send the message
    sock.sendto(MESSAGE2, address_tuple)
    ch.basic_publish(exchange="", routing_key=Channel2, body=MESSAGE2)
    # print a message to the console for the user
    print(f" [x] Sent Food A Temp {MESSAGE2}")
except ValueError:
        pass

try:
    Channel3 = round(float(Channel3),1)
    # use an fstring to create a message from our data
    # f before the opening quote
    Food_B = f"[{Time_UTC_}, {Channel3}]"
    # prepare a binary (1s and 0s) message to stream
    MESSAGE3 = Food_B.encode()
    # use the socket sendto() method to send the message
    sock.sendto(MESSAGE3, address_tuple)
    ch.basic_publish(exchange="", routing_key=Channel3, body=MESSAGE3)
    # print a message to the console for the user
    print(f" [x] Sent Food B Temp {MESSAGE3}")
except ValueError:
        pass
except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
finally:
        # close the connection to the server
        conn.close()
time.sleep(30)

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    # ask the user if they'd like to open the RabbitMQ Admin site
    show_offer = True
    offer_rabbitmq_admin_site(show_offer)
    # get the message from the command line
    # if no arguments are provided, use the default message
    # use the join method to convert the list of arguments into a string
    # join by the space character inside the quotes
    message = " ".join(sys.argv[1:]) or "{MESSAGE}"
    # send the message to the queue
    send_message ("localhost", "01-smoker_temps", "02-Food_A", "03-Food_B", message)
