from flask import Flask
import json, io, re

from tabela.model import TabelaModel
app = Flask(__name__)

class TabelaController:
    def __init__(self):
        pass

    def tabela(self, ano):
        content = TabelaModel().tabela_anual(ano)
        return app.response_class(response=json.dumps(content),status=200,mimetype="application/json")