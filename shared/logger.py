from datetime import datetime


global path

path=".\logs\\"

def error_loger(msg):
    with open(path+"error.txt",'a') as file:
        file.write(msg+'\n')

def info_loger(msg):
    with open(path+"log.txt",'a') as file:
        file.write(msg)
   

def logger(msg, subject='INFO'):
    msg=f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} : {subject} : {msg}\n'
    print(msg)
    if subject=='Error':
        error_loger(msg)
    info_loger(msg)
    # "%m/%d/%Y, %H:%M:%S"