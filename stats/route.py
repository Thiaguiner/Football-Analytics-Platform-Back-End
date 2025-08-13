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

@routes.route('/api/confrontos', methods=['GET'])
def confrontos():
    time1 = request.args.get("time1", default='', type=str)
    time2 = request.args.get("time2", default='', type=str)
    print("time1", time1)
    print("time2", time2)
    return Controller().confrontos_controller(time1, time2)
    