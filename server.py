from flask import Flask, jsonify, request
from flask_cors import CORS
from stats.route import tabela_bp

app = Flask(__name__)

app.register_blueprint(tabela_bp)
CORS(app)

@app.route("/api/hello", methods=["GET"])
def hello():
    return jsonify({"message": "Olá do Flask!"})


if __name__ == "__main__":
    app.run(debug=True, port=5000)
