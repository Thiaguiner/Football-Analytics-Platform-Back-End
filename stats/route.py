from flask import Blueprint, request
from .controller import Controller
from flask_cors import CORS

routes = Blueprint('routes', __name__)
CORS(routes)  # libera CORS só para as rotas desse blueprint 

@routes.route('/api/tabela', methods=['GET'])
def tabela():
    ano = request.args.get("ano", default=2023, type=int)
    print("Ano recebido:", ano)
    return Controller().tabela(ano)


@routes.route('/api/info', methods=['GET'])
def infoTimes():
    time = request.args.get("time", default='Flamengo', type=str)
    print("time", time)
    return Controller().info(time)
    