from flask import Blueprint, request
from .controller import Controller
from flask_cors import CORS

tabela_bp = Blueprint('tabela_bp', __name__)
CORS(tabela_bp)  # libera CORS só para as rotas desse blueprint 

@tabela_bp.route('/api/tabela', methods=['GET'])
def tabela():
    ano = request.args.get("ano", default='', type=int)
    print("Ano recebido:", ano)
    return Controller().tabela(ano)