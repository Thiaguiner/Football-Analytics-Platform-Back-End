from flask import Flask
import json, io, re

from stats.model import Model
app = Flask(__name__)

class Controller:
    def __init__(self):
        pass

    def tabela(self, ano):
        content = Model().tabela_anual(ano)
        print(content)
        
        return app.response_class(response=json.dumps(content),status=200,mimetype="application/json")
    
    def info(self, time):
        content = Model().info_time(time)
        
        return app.response_class(response=json.dumps(content), status=200, mimetype="application/json")
    
    def confrontos_controller(self, time1, time2):
        content = Model().confrontos(time1, time2)
        print(content)
        return app.response_class(response=json.dumps(content), status=200, mimetype="application/json")