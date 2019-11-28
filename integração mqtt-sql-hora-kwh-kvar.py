import paho.mqtt.client as mqtt
import pyodbc
from datetime import datetime
import json

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=fe80::3839:8b34:b3fc:9982%10;'
                      'Database=master;'
                      'Trusted_Connection=yes;')

def on_connect(client, userdata, flags, rc):
    client.subscribe("cultivate")

def on_message(client, userdata, msg):
    content = str(msg.payload, 'utf-8')
    dado = json.loads(content)
    unidade = dado['unidade']
    valor = dado['valor']
    print (unidade)
    print (valor)
    now = datetime.now()
    ano_corrente = str(now.year)
    if(int(now.month) < 10):
        mes_corrente = "0" + str(now.month)
    else:
        mes_corrente = str(now.month)
    if(int(now.day) < 10):
        dia_corrente = "0" + str(now.day)
    else:
        dia_corrente = str(now.day)
    if(int(now.hour) < 10):
        hora_corrente = "0" + str(now.hour)
    else:
        hora_corrente = str(now.hour)
    if(int(now.minute) < 10):
        minuto_corrente = "0" + str(now.minute)
    else:
        minuto_corrente = str(now.minute)
    if(int(now.second) < 10):
        segundo_corrente = "0" + str(now.second)
    else:
        segundo_corrente = str(now.second)

    data = dia_corrente + "/" + mes_corrente + "/" + ano_corrente
    hora = hora_corrente + ":" + minuto_corrente + ":" + segundo_corrente
    print (data)
    print (hora)
    cursor = conn.cursor()
    if(unidade == "kwh"):
        cursor.execute("INSERT INTO master.dbo.kwh(data,hora,kwh) VALUES ('{0}','{1}',{2})".format(data, hora, int(valor)))
    elif(unidade == "kvar"):
        cursor.execute("INSERT INTO master.dbo.indutiva(data,hora,indutiva) VALUES ('{0}','{1}',{2})".format(data, hora, int(valor)))
    
    conn.commit()

    

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.username_pw_set("ueeswiqv", password="ramEh1MZ4mVV")

client.connect("soldier.cloudmqtt.com", 10737, 60)

client.loop_forever()
