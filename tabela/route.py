from flask import Blueprint, request
from .controller import TabelaController

tabela_bp = Blueprint('tabela_bp', __name__)

@tabela_bp.route('/api/tabela', methods=['GET'])
def tabela():
    ano = request.args.get("ano", default='', type=int)
    print("Ano recebido:", ano)
    return TabelaController().tabela(ano)